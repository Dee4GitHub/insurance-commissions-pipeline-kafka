namespace Commissions.Core;

public interface INotificationSender
{
    /// Returns true if the notification is now recorded at the sink, whether this call
    /// wrote it or found it already there. A duplicate is SUCCESS, not failure.
    Task<bool> SendAsync(string dedupeKey, string payload, CancellationToken ct);
}