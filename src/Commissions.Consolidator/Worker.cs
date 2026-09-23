namespace Commissions.Consolidator;

public class Worker(
    ILogger<Worker> logger,
    IServiceScopeFactory serviceScopeFactory,
    IOptions<KafkaConfigOptions> kafkaConfigOptions,
    IOptions<ConsolidatorConfigOptions> consolidatorOptions,
    IConsumer<string, string> consumer,
    OffsetTracker offsets,
    IHostApplicationLifetime lifetime) : BackgroundService
{
    private DateTimeOffset _lastSweep = DateTimeOffset.UtcNow;
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topic = kafkaConfigOptions.Value.CalculatedTopic;
        var batchSize = consolidatorOptions.Value.BatchSize;
        var flushInterval = TimeSpan.FromSeconds(consolidatorOptions.Value.FlushIntervalSeconds);
        var maxFailureDuration = TimeSpan.FromMinutes(15);
        var sweepInterval = TimeSpan.FromSeconds(30);
        var consecutiveFailures = 0;
        DateTimeOffset? failingSince = null;

        consumer.Subscribe(topic);
        logger.LogInformation("Subscribed to {Topic}", topic);

        var buffer = new List<ProcessedRow>();
        var batchIds = new HashSet<string>();
        var lastFlush = DateTimeOffset.UtcNow;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var cr = consumer.Consume(TimeSpan.FromSeconds(1));

                if (cr is not null)
                {
                    try
                    {
                        var calculated = JsonSerializer.Deserialize<CommissionCalculated>(cr.Message.Value)
                            ?? throw new InvalidOperationException("Message deserialized to null");

                        buffer.Add(new ProcessedRow
                        {
                            RowId = calculated.RowId,
                            BatchId = calculated.BatchId,
                            BrokerId = calculated.BrokerId,
                            CommissionAmount = calculated.CommissionAmount,
                            EffectiveDate = calculated.EffectiveDate,
                            PeriodKey = calculated.PeriodKey,
                            RateAsOf = calculated.RateAsOf,
                            ProcessedAt = DateTimeOffset.UtcNow
                        });

                        batchIds.Add(calculated.BatchId);
                        offsets.RecordSuccess(cr.TopicPartition, cr.Offset);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex,
                        "Cannot parse message at partition {Partition} offset {Offset}.",
                            cr.Partition.Value, cr.Offset.Value);

                        if (offsets.RecordFailure(cr.TopicPartition, cr.Offset))
                        {
                            consumer.Pause([cr.TopicPartition]);
                            logger.LogWarning(
                                "Paused partition {Partition} at offset {Offset} until it can be dead-lettered",
                                cr.Partition.Value, cr.Offset.Value);
                        }
                    }
                }

                var timeToFlush = DateTimeOffset.UtcNow - lastFlush >= flushInterval;

                if (buffer.Count >= batchSize || (buffer.Count > 0 && timeToFlush))
                {
                    var written = false;

                    try
                    {
                        await WriteAndCommitAsync(buffer, stoppingToken);
                        written = true;
                        consecutiveFailures = 0;
                        failingSince = null;
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        consecutiveFailures++;
                        failingSince ??= DateTimeOffset.UtcNow;

                        logger.LogError(ex,
                            "Write failed ({Failures} in a row), {Count} rows still buffered",
                            consecutiveFailures, buffer.Count);

                        if (DateTimeOffset.UtcNow - failingSince >= maxFailureDuration)
                        {
                            logger.LogCritical(
                                "The database has rejected writes for {Minutes} minutes. Stopping.",
                                maxFailureDuration.TotalMinutes);
                            lifetime.StopApplication();
                            break;
                        }
                    }

                    if (written)
                    {
                        var completed = batchIds.ToList();

                        buffer.Clear();
                        batchIds.Clear();
                        offsets.ClearAfterCommit();
                        lastFlush = DateTimeOffset.UtcNow;

                        await MarkCompleteAsync(completed, stoppingToken);
                    }
                    else
                    {
                        try
                        {
                            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                    }
                }

                if (consecutiveFailures == 0 && DateTimeOffset.UtcNow - _lastSweep >= sweepInterval)
                {
                    _lastSweep = DateTimeOffset.UtcNow;
                    await SweepStuckBatchesAsync(stoppingToken);
                }
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task SweepStuckBatchesAsync(CancellationToken ct)
    {
        try
        {
            using var scope = serviceScopeFactory.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<ICommissionRepository>();

            var completable = await repository.FindCompletableBatchesAsync(ct);

            if (completable.Count > 0)
            {
                logger.LogInformation(
                    "Sweep found {Count} batches with all rows written but not marked complete",
                    completable.Count);

                await MarkCompleteAsync(completable.ToList(), ct);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Batch completion sweep failed");
        }
    }

    private async Task WriteAndCommitAsync(List<ProcessedRow> buffer, CancellationToken ct)
    {
        using var scope = serviceScopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<ICommissionRepository>();

        await repository.UpsertBatchAsync(buffer, ct);

        var toCommit = offsets.CalculateCommitOffsets();

        if (toCommit.Count > 0)
        {
            consumer.Commit(toCommit);
        }

        logger.LogInformation("Wrote {Count} rows", buffer.Count);
    }

    private async Task MarkCompleteAsync(List<string> batchIds, CancellationToken ct)
    {
        try
        {
            using var scope = serviceScopeFactory.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<ICommissionRepository>();

            foreach (var batchId in batchIds)
            {
                if (ct.IsCancellationRequested)
                {
                    break;
                }

                try
                {
                    await using var tx = await repository.BeginTransactionAsync(ct);

                    if (!await repository.TryMarkBatchCompleteAsync(batchId, tx, ct))
                    {
                        continue;   // not ready, or someone else won. Dispose rolls back.
                    }

                    var summary = await repository.GetBatchSummaryAsync(batchId, tx, ct);

                    await repository.AddOutboxMessageAsync(
                        BatchNotification.From(summary), tx, ct);

                    await tx.CommitAsync(ct);

                    logger.LogInformation(
                        "Batch {BatchId} is COMPLETE, notification queued", batchId);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Could not check completion for batch {BatchId}", batchId);
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Could not check batch completion");
        }
    }
}
