using Commissions.Core;
using Commissions.Infrastructure;
using Microsoft.EntityFrameworkCore;

namespace Commissions.IntegrationTests;

public class BackstopQueryTests : IClassFixture<DatabaseFixture>
{
    // The real database already holds batches the backstop will find,  and the query takes
    // the OLDEST first.  A large max makes sure the test batches are inside the result.
    private const int Max = 10_000;
    private readonly DatabaseFixture _fixture;

    public BackstopQueryTests(DatabaseFixture fixture) => _fixture = fixture;

    private string NewBatch() => $"{_fixture.RunPrefix}-{Guid.NewGuid():N}";

    // Seeds one batch, and optionally an outbox row for it in the given status.
    private async Task<string> SeedAsync(string batchStatus, OutboxStatus? outboxStatus = null)
    {
        var batchId = NewBatch();

        await using var db = _fixture.CreateContext();

        db.Batches.Add(new Batch
        {
            BatchId = batchId,
            FileName = "itest.csv",
            AgencyId = "ITEST",
            AgentEmail = "itest@example.com",
            UploadedAt = DateTimeOffset.UtcNow,
            ExpectedRowCount = 1,
            RejectedRowCount = 0,
            Status = batchStatus,
            CompletedAt = batchStatus == "Complete" ? DateTimeOffset.UtcNow : null,
            PeriodKey = "2026-08",
            IsPeriodCoherent = true
        });

        db.ProcessedRows.Add(new ProcessedRow
        {
            RowId = "ROW-1",
            BatchId = batchId,
            BrokerId = "B100",
            CommissionAmount = 12.34m,
            ProcessedAt = DateTimeOffset.UtcNow,
            EffectiveDate = new DateTimeOffset(2026, 8, 14, 0, 0, 0, TimeSpan.Zero),
            PeriodKey = "2026-08",
            RateAsOf = DateTimeOffset.UtcNow
        });

        if (outboxStatus is not null)
        {
            db.OutboxMessages.Add(new OutboxMessage
            {
                MessageType = "BatchCompletedNotification",
                AggregateId = batchId,
                DedupeKey = $"batch-completed:{batchId}",
                Payload = "{}",
                OccurredAt = DateTimeOffset.UtcNow,
                AvailableAt = DateTimeOffset.UtcNow,
                Status = outboxStatus.Value,
                AttemptCount = 0
            });
        }

        await db.SaveChangesAsync();
        return batchId;

    }
    private async Task<IReadOnlyList<string>> FindAsync()
    {
        await using var db = _fixture.CreateContext();
        return await new OutboxRepository(db).FindUnenqueuedCompleteBatchesAsync(Max, default);
    }

    // Runs the backstop's real enqueue path, then checks the batch is no longer a candidate
    // and that exactly one row exists, with the DedupeKey format the Consolidator uses.
    [Fact]
    public async Task Once_enqueued_a_batch_is_not_found_again()
    {
        var batchId = await SeedAsync("Complete");

        await using (var db = _fixture.CreateContext())
        {
            var commissions = new CommissionsRepository(db);

            await using var tx = await commissions.BeginTransactionAsync(default);
            var summary = await commissions.GetBatchSummaryAsync(batchId, tx, default);
            await commissions.AddOutboxMessageAsync(BatchNotification.From(summary), tx, default);
            await tx.CommitAsync();
        }

        Assert.DoesNotContain(batchId, await FindAsync());

        await using var verify = _fixture.CreateContext();
        var rows = await verify.OutboxMessages.Where(o => o.AggregateId == batchId).ToListAsync();

        var row = Assert.Single(rows);
        Assert.Equal($"batch-completed:{batchId}", row.DedupeKey);
    }

    // THE SPEC_04 DEFECT. A Suppressed row never stamps NotifiedAt, so a rule that only
    // excluded Pending and Sent rows would pick this batch up on every scan, forever.
    [Fact]
    public async Task A_batch_with_a_suppressed_row_is_not_found()
    {
        var batchId = await SeedAsync("Complete", OutboxStatus.Suppressed);

        Assert.DoesNotContain(batchId, await FindAsync());
    }

    // B1: a dead notification is retried by a person, not by the backstop.
    [Fact]
    public async Task A_batch_with_a_dead_row_is_not_found()
    {
        var batchId = await SeedAsync("Complete", OutboxStatus.Dead);

        Assert.DoesNotContain(batchId, await FindAsync());
    }

    [Fact]
    public async Task A_batch_that_is_not_complete_is_not_found()
    {
        var batchId = await SeedAsync("Processing");

        Assert.DoesNotContain(batchId, await FindAsync());
    }
}