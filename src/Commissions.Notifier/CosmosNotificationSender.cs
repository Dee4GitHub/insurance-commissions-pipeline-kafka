using System.Net;
using Microsoft.Azure.Cosmos;

namespace Commissions.Notifier;

public class CosmosNotificationSender(
    Container container,
    ILogger<CosmosNotificationSender> logger) : INotificationSender
{
    public async Task<bool> SendAsync(string dedupeKey, string payload, CancellationToken ct)
    {
        var batchId = ExtractBatchId(dedupeKey);

        var document = new NotificationDocument
        {
            Id = dedupeKey,          // DETERMINISTIC. This is the layer-2 dedupe key.
            BatchId = batchId,
            Payload = payload,
            SentAt = DateTimeOffset.UtcNow
        };

        try
        {
            await container.CreateItemAsync(
                document, new PartitionKey(batchId), cancellationToken: ct);

            return true;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
        {
            // THE MOST IMPORTANT BRANCH IN THIS WORKER.
            // A document with this id already exists, so the notification was already
            // sent - almost certainly by a previous attempt that died after sending but
            // before recording it. The desired end state is reached, so this is SUCCESS.
            // Treating it as a failure would retry forever and never converge.
            logger.LogInformation(
                "Notification for {DedupeKey} already exists. A previous attempt sent it; " +
                "treating as success.", dedupeKey);

            return true;
        }
    }

    private static string ExtractBatchId(string dedupeKey)
    {
        var separator = dedupeKey.IndexOf(':');
        return separator >= 0 ? dedupeKey[(separator + 1)..] : dedupeKey;
    }
}