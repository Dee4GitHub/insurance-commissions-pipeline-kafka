namespace Commissions.Lookup;

public class Worker(
    ILogger<Worker> logger,
    IServiceScopeFactory serviceScopeFactory,
    IOptions<KafkaConfigOptions> kafkaConfigOptions,
    IMessagePublisher publisher,
    IConsumer<string, string> consumer) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var rawTopic = kafkaConfigOptions.Value.RawTopic;
        var enrichedTopic = kafkaConfigOptions.Value.EnrichedTopic;

        consumer.Subscribe(rawTopic);
        logger.LogInformation("Subscribed to {Topic}", rawTopic);

        while (!stoppingToken.IsCancellationRequested)
        {
            var cr = consumer.Consume(TimeSpan.FromSeconds(1));
            if (cr is null) continue;

            try
            {
                var raw = JsonSerializer.Deserialize<CommissionRaw>(cr.Message.Value)
                    ?? throw new InvalidOperationException("Message deserialized to null");

                using var scope = serviceScopeFactory.CreateScope();
                var lookup = scope.ServiceProvider.GetRequiredService<IBrokerTierLookup>();

                var (tier, multiplier) = await lookup.GetTierAsync(raw.BrokerId, stoppingToken);

                var enriched = new CommissionEnriched(
                    raw.RowId, raw.BrokerId, raw.PolicyNumber,
                    raw.PremiumAmount, raw.CommissionRate,
                    tier, multiplier, raw.BatchId, raw.EffectiveDate,
                    raw.PeriodKey, DateTimeOffset.UtcNow);

                await publisher.PublishAsync(enrichedTopic, raw.RowId, enriched, stoppingToken);

                consumer.Commit(cr);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed row at partition {Partition} offset {Offset}",
                    cr.Partition.Value, cr.Offset.Value);
                consumer.Commit(cr);
            }
        }

        consumer.Close();
    }
}
