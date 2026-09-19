namespace Commissions.Infrastructure;
public class KafkaMessagePublisher(
    IProducer<string, string> producer,
    ILogger<KafkaMessagePublisher> logger) : IMessagePublisher
{
    public async Task PublishAsync<T>(string topic, string key, T message, CancellationToken ct)
    {
        var json = JsonSerializer.Serialize(message);

        var result = await producer.ProduceAsync(topic, new Message<string, string> {
             Key = key,
             Value = json
           }, ct);

        logger.LogInformation(
            "Published key {Key} to partition {Partition} at offset {Offset}",
            key, result.Partition.Value, result.Offset.Value);
    }
}