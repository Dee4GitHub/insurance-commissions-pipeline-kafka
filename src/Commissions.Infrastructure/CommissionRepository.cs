namespace Commissions.Infrastructure;
public class CommissionsRepository : ICommissionRepository
{
    private readonly CommissionsDBContext _dbContext;

    public CommissionsRepository(CommissionsDBContext dbContext)
    {
        _dbContext = dbContext;
    }

public async Task UpsertAsync(ProcessedRow row, CancellationToken ct)
{
    await _dbContext.Database.ExecuteSqlInterpolatedAsync($@"
        MERGE INTO ProcessedRows WITH (HOLDLOCK) AS T
        USING (VALUES ({row.RowId}, {row.BatchId}, {row.BrokerId},
                       {row.CommissionAmount}, {row.ProcessedAt}))
              AS S (RowId, BatchId, BrokerId, CommissionAmount, ProcessedAt)
        ON  T.RowId   = S.RowId
        AND T.BatchId = S.BatchId
        WHEN MATCHED THEN
            UPDATE SET BrokerId         = S.BrokerId,
                       CommissionAmount = S.CommissionAmount,
                       ProcessedAt      = S.ProcessedAt
        WHEN NOT MATCHED THEN
            INSERT (RowId, BatchId, BrokerId, CommissionAmount, ProcessedAt)
            VALUES (S.RowId, S.BatchId, S.BrokerId, S.CommissionAmount, S.ProcessedAt);", ct);
}

    public async Task<int> CountForBatchAsync(string batchId, CancellationToken ct)
    {
        return await _dbContext.ProcessedRows.CountAsync(r => r.BatchId == batchId, ct);
    }
}