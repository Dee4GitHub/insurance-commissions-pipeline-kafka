
namespace Commissions.Core;

public interface ICommissionRepository
{
    Task UpsertAsync(ProcessedRow row, CancellationToken ct);
    Task<int> CountForBatchAsync(string batchId, CancellationToken ct);
}