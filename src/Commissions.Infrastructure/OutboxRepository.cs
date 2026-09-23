namespace Commissions.Infrastructure;

public class OutboxRepository : IOutboxRepository
{
    private readonly CommissionsDBContext _dbContext;

    public OutboxRepository(CommissionsDBContext dbContext) => _dbContext = dbContext;

    // ONE statement. A SELECT followed by an UPDATE has a window in which two drainers
    // read the same rows and both proceed. READPAST skips rows another drainer is
    // mid-claim on rather than blocking behind them.
    // AttemptCount increments HERE, at claim time - a process that dies mid-send never
    // reaches the failure path, and an uncounted attempt can repeat forever.
    public async Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(
        int batchSize,
        int leaseSeconds,
        string instanceId,
        CancellationToken ct)
    {
        return await _dbContext.OutboxMessages
            .FromSql($@"
                UPDATE TOP ({batchSize}) OutboxMessages WITH (READPAST)
                SET    LockedUntil  = DATEADD(second, {leaseSeconds}, SYSDATETIMEOFFSET()),
                       LockedBy     = {instanceId},
                       AttemptCount = AttemptCount + 1
                OUTPUT inserted.*
                WHERE  Status = 0
                  AND  AvailableAt <= SYSDATETIMEOFFSET()
                  AND  (LockedUntil IS NULL OR LockedUntil < SYSDATETIMEOFFSET())")
            .AsNoTracking()
            .ToListAsync(ct);
    }

    public async Task MarkSentAsync(long outboxId, string aggregateId, CancellationToken ct)
    {
        await _dbContext.Database.ExecuteSqlInterpolatedAsync($@"
            UPDATE OutboxMessages
            SET    Status = 1, SentAt = SYSDATETIMEOFFSET(),
                   LockedUntil = NULL, LockedBy = NULL
            WHERE  OutboxId = {outboxId};

            UPDATE Batches
            SET    NotifiedAt = SYSDATETIMEOFFSET()
            WHERE  BatchId = {aggregateId} AND NotifiedAt IS NULL;", ct);
    }

    public async Task MarkFailedAsync(long outboxId, string error, int backoffSeconds, CancellationToken ct)
    {
        var truncated = error.Length > 2000 ? error[..2000] : error;

        await _dbContext.Database.ExecuteSqlInterpolatedAsync($@"
            UPDATE OutboxMessages
            SET    AvailableAt = DATEADD(second, {backoffSeconds}, SYSDATETIMEOFFSET()),
                   LockedUntil = NULL, LockedBy = NULL,
                   LastError   = {truncated}
            WHERE  OutboxId = {outboxId}", ct);
    }

    public async Task MarkDeadAsync(long outboxId, string error, CancellationToken ct)
    {
        var truncated = error.Length > 2000 ? error[..2000] : error;

        await _dbContext.Database.ExecuteSqlInterpolatedAsync($@"
            UPDATE OutboxMessages
            SET    Status = 2, FailedAt = SYSDATETIMEOFFSET(),
                   LockedUntil = NULL, LockedBy = NULL,
                   LastError   = {truncated}
            WHERE  OutboxId = {outboxId}", ct);
    }

    public async Task MarkSuppressedAsync(long outboxId, string reason, CancellationToken ct)
    {
        await _dbContext.Database.ExecuteSqlInterpolatedAsync($@"
            UPDATE OutboxMessages
            SET    Status = 3, LockedUntil = NULL, LockedBy = NULL,
                   LastError = {reason}
            WHERE  OutboxId = {outboxId}", ct);
    }

    public async Task<bool> IsPeriodOpenAsync(string periodKey, CancellationToken ct)
    {
        return await _dbContext.Periods
            .AsNoTracking()
            .AnyAsync(p => p.PeriodKey == periodKey && p.Status == PeriodStatus.Open, ct);
    }

    // Completed batches that have NO outbox row in ANY status (B1). A Suppressed or Dead
    // row means the batch was already handled - re-enqueueing it would only hit the unique
    // DedupeKey index on every scan. Matched on AggregateId because it is indexed.
    // Oldest first, and bounded, so one scan cannot build thousands of summaries.
    public async Task<IReadOnlyList<string>> FindUnenqueuedCompleteBatchesAsync(
        int max, CancellationToken ct)
    {
        return await _dbContext.Batches
            .AsNoTracking()
            .Where(b => b.Status == "Complete"
                     && b.NotifiedAt == null
                     && !_dbContext.OutboxMessages.Any(o => o.AggregateId == b.BatchId))
            .OrderBy(b => b.CompletedAt)
            .Select(b => b.BatchId)
            .Take(max)
            .ToListAsync(ct);
    }
}