using Commissions.Core;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;

namespace Commissions.IntegrationTests;

public class OutboxTransactionsTests : IClassFixture<DatabaseFixture>
{
    private readonly DatabaseFixture _fixture;

    public OutboxTransactionsTests(DatabaseFixture fixture) => _fixture = fixture;

    //Each test gets its own batch id so tests cannot collide with each other.
    private string NewBatchId() => $"{_fixture.RunPrefix}-{Guid.NewGuid():N}";

    private static ProcessedRow NewRow(string batchId) => new()
    {
        RowId = "ROW-1",
        BatchId = batchId,
        BrokerId = "B100",
        CommissionAmount = 12.34m,
        ProcessedAt = DateTimeOffset.UtcNow,
        EffectiveDate = new DateTimeOffset(2026, 8, 14, 0, 0, 0, TimeSpan.Zero),
        PeriodKey = "2026-08",
        RateAsOf = DateTimeOffset.UtcNow
    };

    private static OutboxMessage NewMessage(string batchId, string dedupeKey) => new()
    {
        MessageType = "BatchCompletedNotification",
        AggregateId = batchId,
        DedupeKey = dedupeKey,
        Payload = "{}",
        OccurredAt = DateTimeOffset.UtcNow,
        AvailableAt = DateTimeOffset.UtcNow,
        Status = OutboxStatus.Pending,
        AttemptCount = 0
    };

    [Fact]
    public async Task Rollback_discards_the_bulk_write_and_the_outbox_row_together()
    {
        var batchId = NewBatchId();

        await using (var db = _fixture.CreateContext())
        {
            await using var tx = await db.Database.BeginTransactionAsync();

            await db.BulkInsertOrUpdateAsync(new List<ProcessedRow> { NewRow(batchId) });
            db.OutboxMessages.Add(NewMessage(batchId, $"dedupe:{batchId}"));
            await db.SaveChangesAsync();

            await tx.RollbackAsync();
        }

        await using var verify = _fixture.CreateContext();
        Assert.Equal(0, await verify.ProcessedRows.CountAsync(r => r.BatchId == batchId));
        Assert.Equal(0, await verify.OutboxMessages.CountAsync(o => o.AggregateId == batchId));
    }

    // Without this, "0 rows after rollback" could mean the transaction worked OR that
    // neither write ever happened. Those are indistinguishable.
    [Fact]
    public async Task Commit_persists_the_bulk_write_and_the_outbox_row_together()
    {
        var batchId = NewBatchId();

        await using (var db = _fixture.CreateContext())
        {
            await using var tx = await db.Database.BeginTransactionAsync();

            await db.BulkInsertOrUpdateAsync(new List<ProcessedRow> { NewRow(batchId) });
            db.OutboxMessages.Add(NewMessage(batchId, $"dedupe:{batchId}"));
            await db.SaveChangesAsync();

            await tx.CommitAsync();
        }

        await using var verify = _fixture.CreateContext();
        Assert.Equal(1, await verify.ProcessedRows.CountAsync(r => r.BatchId == batchId));
        Assert.Equal(1, await verify.OutboxMessages.CountAsync(o => o.AggregateId == batchId));
    }

    // THE PRODUCTION SHAPE: the data write succeeds and the intent-to-notify does not.
    // This is the exact failure the outbox exists to make impossible.
    [Fact]
    public async Task A_failed_outbox_insert_leaves_no_orphaned_bulk_rows()
    {
        var existingBatchId = NewBatchId();
        var sharedKey = $"dedupe:{existingBatchId}";

        // Land one row first so the second insert has a key to collide with.
        await using (var seed = _fixture.CreateContext())
        {
            seed.OutboxMessages.Add(NewMessage(existingBatchId, sharedKey));
            await seed.SaveChangesAsync();
        }

        var batchId = NewBatchId();

        await using (var db = _fixture.CreateContext())
        {
            await using var tx = await db.Database.BeginTransactionAsync();
            try
            {
                await db.BulkInsertOrUpdateAsync(new List<ProcessedRow> { NewRow(batchId) });

                // Same DedupeKey as the seeded row - violates the unique index.
                db.OutboxMessages.Add(NewMessage(batchId, sharedKey));
                await db.SaveChangesAsync();

                await tx.CommitAsync();
                Assert.Fail("Expected the duplicate DedupeKey to throw.");
            }
            catch (DbUpdateException)
            {
                await tx.RollbackAsync();
            }
        }

        await using var verify = _fixture.CreateContext();
        Assert.Equal(0, await verify.ProcessedRows.CountAsync(r => r.BatchId == batchId));
    }

    [Fact]
    public async Task The_unique_index_rejects_a_duplicate_dedupe_key()
    {
        var batchId = NewBatchId();
        var key = $"dedupe:{batchId}";

        await using (var first = _fixture.CreateContext())
        {
            first.OutboxMessages.Add(NewMessage(batchId, key));
            await first.SaveChangesAsync();
        }

        await using var second = _fixture.CreateContext();
        second.OutboxMessages.Add(NewMessage(batchId, key));

        await Assert.ThrowsAsync<DbUpdateException>(() => second.SaveChangesAsync());
    }
}