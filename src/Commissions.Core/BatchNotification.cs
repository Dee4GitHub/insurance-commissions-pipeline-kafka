using System.Text.Json;

namespace Commissions.Core;

// The ONE place a batch-completed notification is built. The Consolidator and the
// Notifier's backstop both call this, so the DedupeKey format can never drift apart.
public static class BatchNotification
{
    public static OutboxMessage From(BatchSummary summary) => new()
    {
        MessageType = "BatchCompletedNotification",
        AggregateId = summary.BatchId,
        DedupeKey = $"batch-completed:{summary.BatchId}",
        Payload = JsonSerializer.Serialize(summary),
        OccurredAt = summary.CompletedAt,
        AvailableAt = DateTimeOffset.UtcNow,
        Status = OutboxStatus.Pending,
        AttemptCount = 0,
        PeriodKey = summary.PeriodKey,
        IsPeriodCoherent = summary.IsPeriodCoherent
    };
}
