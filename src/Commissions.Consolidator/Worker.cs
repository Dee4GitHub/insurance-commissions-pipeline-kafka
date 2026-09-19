namespace Commissions.Consolidator;

public class Worker(
    ILogger<Worker> logger,
    IServiceScopeFactory serviceScopeFactory,
    IOptions<KafkaConfigOptions> kafkaConfigOptions,
    IOptions<ConsolidatorConfigOptions> consolidatorOptions,
    IConsumer<string, string> consumer) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topic = kafkaConfigOptions.Value.CalculatedTopic;
        var batchSize = consolidatorOptions.Value.BatchSize;
        var flushInterval = TimeSpan.FromSeconds(consolidatorOptions.Value.FlushIntervalSeconds);

        consumer.Subscribe(topic);
        logger.LogInformation("Subscribed to {Topic}", topic);

        var buffer = new List<ProcessedRow>();
        var batchIds = new HashSet<string>();
        var offsets = new Dictionary<TopicPartition, Offset>();
        var lastFlush = DateTimeOffset.UtcNow;

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
                    offsets[cr.TopicPartition] = cr.Offset;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed row at partition {Partition} offset {Offset}",
                        cr.Partition.Value, cr.Offset.Value);
                }
            }

            var timeToFlush = DateTimeOffset.UtcNow - lastFlush >= flushInterval;

            if (buffer.Count >= batchSize || (buffer.Count > 0 && timeToFlush))
            {
                try
                {
                    await FlushAsync(buffer, batchIds, offsets, stoppingToken);
                    buffer.Clear();
                    batchIds.Clear();
                    offsets.Clear();
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Flush failed, {Count} rows still buffered", buffer.Count);
                }

                lastFlush = DateTimeOffset.UtcNow;
            }
        }

        consumer.Close();
    }

    private async Task FlushAsync(
        List<ProcessedRow> buffer,
        HashSet<string> batchIds, 
        Dictionary<TopicPartition, Offset> offsets,
        CancellationToken stoppingToken)
    {
        using var scope = serviceScopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<ICommissionRepository>();
 
        await repository.UpsertBatchAsync(buffer, stoppingToken);
    
        var toCommit = offsets
            .Select(kv => new TopicPartitionOffset(kv.Key, kv.Value + 1))
            .ToList();
    
        consumer.Commit(toCommit);
        
        logger.LogInformation("Wrote {Count} rows", buffer.Count);

        foreach (var batchId in batchIds)
        {
            if (await repository.TryMarkBatchCompleteAsync(batchId, stoppingToken))
            {
                logger.LogInformation("Batch {BatchId} is COMPLETE", batchId);
            }
        }
    }
}
