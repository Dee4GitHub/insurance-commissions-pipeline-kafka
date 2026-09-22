using Microsoft.EntityFrameworkCore.Storage;

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

    public Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken ct)
        => _dbContext.Database.BeginTransactionAsync(ct);

    // The tx parameter is not decoration. EF routes operations through the context's
    // CURRENT transaction, so passing a different one would silently write outside it.
    // This makes that a loud failure instead.
    private void AssertIsCurrent(IDbContextTransaction tx)
    {
        var current = _dbContext.Database.CurrentTransaction;

        if (current is null)
        {
            throw new InvalidOperationException(
                "This call must run inside a transaction, but the context has none. " +
                "The repository and the transaction must share one DbContext.");
        }

        if (!ReferenceEquals(current, tx))
        {
            throw new InvalidOperationException(
                "The transaction passed in is not the context's current transaction. " +
                "The caller is holding a transaction from a different DbContext instance.");
        }
    }

    public async Task<bool> TryMarkBatchCompleteAsync(
        string batchId, IDbContextTransaction tx, CancellationToken ct)
    {
        AssertIsCurrent(tx);

        var affected = await _dbContext.Database.ExecuteSqlInterpolatedAsync($@"
            UPDATE Batches
            SET    Status = 'Complete', CompletedAt = SYSDATETIMEOFFSET()
            WHERE  BatchId = {batchId}
              AND  Status <> 'Complete'
              AND  (SELECT COUNT(*) FROM ProcessedRows WHERE BatchId = {batchId})
                   = ExpectedRowCount", ct);

        return affected == 1;
    }

    public async Task<BatchSummary> GetBatchSummaryAsync(
        string batchId, IDbContextTransaction tx, CancellationToken ct)
    {
        AssertIsCurrent(tx);

        var batch = await _dbContext.Batches
            .AsNoTracking()
            .SingleOrDefaultAsync(b => b.BatchId == batchId, ct)
            ?? throw new InvalidOperationException(
                $"Batch {batchId} was marked complete but could not be read back.");

        var total = await _dbContext.ProcessedRows
            .Where(r => r.BatchId == batchId)
            .SumAsync(r => r.CommissionAmount, ct);

        var rowCount = await _dbContext.ProcessedRows
            .CountAsync(r => r.BatchId == batchId, ct);

        return new BatchSummary(
            batch.BatchId,
            batch.AgentEmail,
            rowCount,
            total,
            batch.CompletedAt ?? DateTimeOffset.UtcNow,
            batch.PeriodKey,
            batch.IsPeriodCoherent);
    }

    public async Task AddOutboxMessageAsync(
        OutboxMessage message, IDbContextTransaction tx, CancellationToken ct)
    {
        AssertIsCurrent(tx);

        _dbContext.OutboxMessages.Add(message);
        await _dbContext.SaveChangesAsync(ct);
    }
}