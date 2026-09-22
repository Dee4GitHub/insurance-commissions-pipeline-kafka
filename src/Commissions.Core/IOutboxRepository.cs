namespace Commissions.Core;

public interface IOutboxRepository
{
    Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(int batchSize, int leaseSeconds, string instanceId, CancellationToken ct);
    Task MarkSentAsync(long outboxId, string aggregateId, CancellationToken ct);
    Task MarkFailedAsync(long outboxId, string error, int backOffSeconds, CancellationToken ct);
    Task MarkDeadAsync(long outboxId, string error, CancellationToken ct);
    Task MarkSuppressedAsync(long outboxId, string reason, CancellationToken ct);
    Task<bool> IsPeriodOpenAsync(string periodKey, CancellationToken ct);
}