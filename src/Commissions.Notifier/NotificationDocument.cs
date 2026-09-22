using Newtonsoft.Json;

namespace Commissions.Notifier;

public class NotificationDocument
{
    // Cosmos SDK v3 serialises with Newtonsoft, NOT System.Text.Json. A
    // [JsonPropertyName] attribute from System.Text.Json is silently ignored here,
    // and the document goes out with "Id" instead of "id".
    [JsonProperty("id")]
    public string Id { get; set; } = default!;

    [JsonProperty("batchId")]
    public string BatchId { get; set; } = default!;

    [JsonProperty("payload")]
    public string Payload { get; set; } = default!;

    [JsonProperty("sentAt")]
    public DateTimeOffset SentAt { get; set; }
}