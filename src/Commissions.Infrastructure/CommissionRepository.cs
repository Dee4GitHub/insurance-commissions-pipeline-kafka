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
        var affected = await _dbContext.Database.ExecuteSqlInterpolatedAsync($@"
            UPDATE Batches
            SET    Status = 'Complete', CompletedAt = SYSDATETIMEOFFSET()
            WHERE  BatchId = {batchId}
              AND  Status <> 'Complete'
              AND  (SELECT COUNT(*) FROM ProcessedRows WHERE BatchId = {batchId})
                   = ExpectedRowCount", ct);

        return affected == 1;
    }

    public async Task<IReadOnlyCollection<string>> FindCompletableBatchesAsync(CancellationToken ct)
    {
        return await _dbContext.Batches
            .AsNoTracking()
            .Where(b => b.Status != "Complete")
            .Where(b => _dbContext.ProcessedRows.Count(p => p.BatchId == b.BatchId) == b.ExpectedRowCount)
            .Select(b => b.BatchId)
            .ToListAsync(ct);
    }
}