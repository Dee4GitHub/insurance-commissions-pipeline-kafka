namespace Commissions.Notifier;

// Wraps the real sender. Sends, then ends the process at once, so the Cosmos document
// exists but the outbox row is never marked Sent. That is the crash window SPEC_04 PART 5
// says is safe, and this is how it is made to happen on purpose.
public class CrashAfterSendNotificationSender(
    CosmosNotificationSender inner,
    ILogger<CrashAfterSendNotificationSender> logger) : INotificationSender
{
    public async Task<bool> SendAsync(string dedupeKey, string payload, CancellationToken ct)
    {
        var result = await inner.SendAsync(dedupeKey, payload, ct);

        logger.LogCritical(
            "CrashAfterSend: {DedupeKey} was sent. Killing the process BEFORE it is marked Sent.",
            dedupeKey);

        // No finally blocks, no disposal, no graceful shutdown - the nearest thing to a
        // power cut a .NET process can do to itself.
        Environment.FailFast($"CrashAfterSend test hook after sending {dedupeKey}");

        return result;   // never reached
    }
}