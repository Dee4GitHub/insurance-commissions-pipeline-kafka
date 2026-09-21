
using Microsoft.EntityFrameworkCore.Storage;

namespace Commissions.Core;

public interface ICommissionRepository
{
    Task UpsertBatchAsync(IReadOnlyCollection<ProcessedRow> rows, CancellationToken ct);
    Task<int> CountForBatchAsync(string batchId, CancellationToken ct);
    Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken ct);
    Task<bool> TryMarkBatchCompleteAsync(string batchId, IDbContextTransaction tx, CancellationToken ct);
    Task<BatchSummary> GetBatchSummaryAsync(string batchId, IDbContextTransaction tx, CancellationToken ct);
    Task AddOutboxMessageAsync(OutboxMessage message, IDbContextTransaction tx, CancellationToken ct);
    Task<IReadOnlyCollection<string>> FindCompletableBatchesAsync(CancellationToken ct);
}