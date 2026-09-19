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
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topic = kafkaConfigOptions.Value.CalculatedTopic;
        var batchSize = consolidatorOptions.Value.BatchSize;
        var maxBuffered = batchSize * 10;
        var flushInterval = TimeSpan.FromSeconds(consolidatorOptions.Value.FlushIntervalSeconds);

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
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Write failed, {Count} rows still buffered", buffer.Count);

                        if (buffer.Count >= maxBuffered)
                        {
                            logger.LogCritical(
                                "Buffer has reached {Count} rows and the database is not accepting writes. Stopping.",
                                buffer.Count);
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
            }
        }
        finally
        {
            consumer.Close();
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
        using var scope = serviceScopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<ICommissionRepository>();

        foreach (var batchId in batchIds)
        {
            try
            {
                if (await repository.TryMarkBatchCompleteAsync(batchId, ct))
                {
                    logger.LogInformation("Batch {BatchId} is COMPLETE", batchId);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Could not check completion for batch {BatchId}", batchId);
            }
        }
    }
}
