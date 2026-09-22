namespace Commissions.Calculator;

public class Worker(
    ILogger<Worker> logger,
    IOptions<KafkaConfigOptions> kafkaConfigOptions,
    IMessagePublisher publisher,
    IConsumer<string, string> consumer) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var enrichedTopic = kafkaConfigOptions.Value.EnrichedTopic;
        var calculatedTopic = kafkaConfigOptions.Value.CalculatedTopic;

        consumer.Subscribe(enrichedTopic);
        logger.LogInformation("Subscribed to {Topic}", enrichedTopic);

        while (!stoppingToken.IsCancellationRequested)
        {
            var cr = consumer.Consume(TimeSpan.FromSeconds(1));
            if (cr is null) continue;

            try
            {
                var enriched = JsonSerializer.Deserialize<CommissionEnriched>(cr.Message.Value)
                                ?? throw new InvalidOperationException("Message deserialized to null");

                var amount = CommissionCalculator.CalculateCommission(enriched.PremiumAmount, enriched.CommissionRate, enriched.TierMultiplier);

                var calculated = new CommissionCalculated(enriched.RowId,
                enriched.BrokerId, enriched.PolicyNumber,
                amount, enriched.BatchId,
                enriched.EffectiveDate, enriched.PeriodKey,
                enriched.RateAsOf);

                await publisher.PublishAsync(calculatedTopic, enriched.RowId, calculated, stoppingToken);
                consumer.Commit(cr);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed row at partition {Partition} offset {Offset}", cr.Partition.Value, cr.Offset.Value);
                consumer.Commit(cr);
            }
        }
        consumer.Close();
    }
}
