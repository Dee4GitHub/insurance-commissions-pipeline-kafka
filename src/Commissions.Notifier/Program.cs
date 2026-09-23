var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<Worker>();

builder.Services.AddOptions<NotifierConfigOptions>()
    .Bind(builder.Configuration.GetSection(NotifierConfigOptions.SectionName))
    .Validate(o => o.DrainIntervalSeconds > 0, "DrainIntervalSeconds must be greater than zero.")
    .Validate(o => o.BackstopIntervalSeconds > 0, "BackstopIntervalSeconds must be greater than zero.")
    .Validate(o => o.BatchSize > 0, "BatchSize must be greater than zero.")
    .Validate(o => o.LeaseSeconds > 0, "LeaseSeconds must be greater than zero.")
    .Validate(o => o.MaxAttempts > 0, "MaxAttempts must be greater than zero.")
    .Validate(o => o.MaxBackoffSeconds >= o.BaseBackoffSeconds,
              "MaxBackoffSeconds must be at least BaseBackoffSeconds.")
    .ValidateOnStart();
builder.Services.AddSingleton(sp =>
{
    var connectionString = builder.Configuration.GetConnectionString("Cosmos");
    return new CosmosClient(connectionString, new CosmosClientOptions
    {
        // The vnext-preview emulator does not support Direct mode - database creation
        // fails with a 400 on a malformed addresses call. Gateway mode works.
        ConnectionMode = ConnectionMode.Gateway,
        ServerCertificateCustomValidationCallback = (_, _, _) => true
    });
});

builder.Services.AddSingleton(sp =>
    sp.GetRequiredService<CosmosClient>()
      .GetContainer("commissions", "notifications"));

builder.Services.AddScoped<INotificationSender, CosmosNotificationSender>();
builder.Services.AddDbContext<CommissionsDBContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("CommissionsDb")));
builder.Services.AddScoped<IOutboxRepository, OutboxRepository>();

// The backstop reuses the Consolidator's transaction path (BeginTransactionAsync,
// GetBatchSummaryAsync, AddOutboxMessageAsync), so it needs the same repository.
builder.Services.AddScoped<ICommissionRepository, CommissionsRepository>();
var host = builder.Build();

// The emulator starts empty, so create the database and container if they are not there.
// Partition key is /batchId: one notification per batch, so it spreads evenly.
// Using /messageType would put every document in one partition - the same mistake as
// keying Kafka messages on a low-cardinality field.
using (var scope = host.Services.CreateScope())
{
    var client = scope.ServiceProvider.GetRequiredService<CosmosClient>();

    var database = await client.CreateDatabaseIfNotExistsAsync("commissions");
    await database.Database.CreateContainerIfNotExistsAsync("notifications", "/batchId");
}

host.Run();
