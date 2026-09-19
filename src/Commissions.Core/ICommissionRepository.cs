
namespace Commissions.Core;

public interface ICommissionRepository
{
    Task UpsertBatchAsync(IReadOnlyCollection<ProcessedRow> rows, CancellationToken ct);
    Task<int> CountForBatchAsync(string batchId, CancellationToken ct);
    Task<bool> TryMarkBatchCompleteAsync(string batchId, CancellationToken ct);
    Task<IReadOnlyCollection<string>> FindCompletableBatchesAsync(CancellationToken ct);
}