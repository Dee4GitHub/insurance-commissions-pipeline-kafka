namespace Commissions.Ingestor;

public class KafkaConfigOptions
{
    public const string SectionName = "Kafka";
    public string BootstrapServers { get; set; } = default!;
    public string EnrichedTopic { get; set; } = default!;
    public string ConsumerGroupId { get; set; } = default!;
    public string CalculatedTopic {get; set;} = default!;    
}