using Commissions.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace Commissions.IntegrationTests;

public class DatabaseFixture : IAsyncLifetime
{
    private readonly DbContextOptions<CommissionsDBContext> _options;
    public string RunPrefix { get; } = $"ITEST-{Guid.NewGuid():N}";

    public DatabaseFixture()
    {
        var config = new ConfigurationBuilder()
                        .AddUserSecrets<DatabaseFixture>()
                        .Build();
        var connectionString = config.GetConnectionString("CommissionsDb");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                "No CommissionsDb connection string. Run: dotnet user-secrets set" +
                "\"ConnectionStrings:CommissionsDb\" \"...\" --project tests/Commissions.IntegrationTests");
        }

        _options = new DbContextOptionsBuilder<CommissionsDBContext>()
                   .UseSqlServer(connectionString)
                   .Options;
    }
    // A FRESH context per call. EF caches entities it has already seen, so verifying
    // a rollback through the same context that did the write can read the cached copy
    // instead of the database.
    public CommissionsDBContext CreateContext() => new(_options);
    public Task InitializeAsync() => Task.CompletedTask;
    public async Task DisposeAsync()
    {
        await using var db = CreateContext();

        //Childresn before parents - ProcessedRows has a composite key including BatchId.
        await db.ProcessedRows
                 .Where(r => r.BatchId.StartsWith(RunPrefix))
                 .ExecuteDeleteAsync();
        await db.OutboxMessages
                 .Where(o => o.AggregateId.StartsWith(RunPrefix))
                 .ExecuteDeleteAsync();
        await db.Batches
              .Where(b => b.BatchId.StartsWith(RunPrefix))
              .ExecuteDeleteAsync();
    }
}