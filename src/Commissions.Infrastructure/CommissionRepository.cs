namespace Commissions.Infrastructure;
public class CommissionsRepository : ICommissionRepository
{
    private readonly CommissionsDBContext _dbContext;

    public CommissionsRepository(CommissionsDBContext dbContext)
    {
        _dbContext = dbContext;
    }

    public async Task UpsertBatchAsync(IReadOnlyCollection<ProcessedRow> rows, CancellationToken ct)
        => await _dbContext.BulkInsertOrUpdateAsync(rows, cancellationToken: ct);

    public async Task<int> CountForBatchAsync(string batchId, CancellationToken ct)
    {
        return await _dbContext.ProcessedRows.CountAsync(r => r.BatchId == batchId, ct);
    }

    public async Task<bool> TryMarkBatchCompleteAsync(string batchId, CancellationToken ct)
    {
        var batch = await _dbContext.Batches
            .AsNoTracking()
            .FirstOrDefaultAsync(b => b.BatchId == batchId, ct);

        if (batch is null)
        {
            return false;
        }

        var processedCount = await CountForBatchAsync(batchId, ct);

        if (processedCount < batch.ExpectedRowCount)
        {
            return false;
        }

        var affected = await _dbContext.Batches
            .Where(b => b.BatchId == batchId && b.Status != "Complete")
            .ExecuteUpdateAsync(s => s
                .SetProperty(b => b.Status, "Complete")
                .SetProperty(b => b.CompletedAt, DateTimeOffset.UtcNow), ct);

        return affected == 1;         
    }
}