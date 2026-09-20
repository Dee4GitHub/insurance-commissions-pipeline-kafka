
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();
builder.Services.AddOptions<KafkaConfigOptions>()
    .Bind(builder.Configuration.GetSection(KafkaConfigOptions.SectionName))
    .Validate(o => !string.IsNullOrWhiteSpace(o.BootstrapServers), "BootstrapServers must be provided.")
    .Validate(o => !string.IsNullOrWhiteSpace(o.BootstrapServers), "BootstrapServers must be provided.")
    .Validate(o => !string.IsNullOrWhiteSpace(o.ConsumerGroupId), "ConsumerGroupId must be provided.")
    .Validate(o => !string.IsNullOrWhiteSpace(o.BootstrapServers), "BootstrapServers must be provided.")
    .ValidateOnStart();
builder.Services.AddOptions<ConsolidatorConfigOptions>()
    .Bind(builder.Configuration.GetSection(ConsolidatorConfigOptions.SectionName))
    .Validate(o => o.BatchSize > 0, "BatchSize must be greater than zero.")
    .Validate(o => o.FlushIntervalSeconds > 0, "FlushIntervalSeconds must be greater than zero.")
    .ValidateOnStart();

builder.Services.AddSingleton<OffsetTracker>();

builder.Services.AddSingleton<IConsumer<string, string>>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<KafkaConfigOptions>>().Value;
    var tracker = sp.GetRequiredService<OffsetTracker>();

    return new ConsumerBuilder<string, string>(new ConsumerConfig
    {
        BootstrapServers = opts.BootstrapServers,
        GroupId = opts.ConsumerGroupId,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false,
        EnableAutoOffsetStore = false
    })
    .SetPartitionsRevokedHandler((_, revoked) =>
        tracker.ForgetPartitions(revoked.Select(r => r.TopicPartition)))
    .Build();
});

builder.Services.AddSingleton<IProducer<string, string>>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<KafkaConfigOptions>>().Value;
    return new ProducerBuilder<string, string>(
        new ProducerConfig { BootstrapServers = opts.BootstrapServers }).Build();
});
builder.Services.AddSingleton<IMessagePublisher, KafkaMessagePublisher>();
builder.Services.AddScoped<IBrokerTierLookup, BrokerTierLookup>();
builder.Services.AddDbContext<CommissionsDBContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("CommissionsDb")));
builder.Services.AddScoped<ICommissionRepository, CommissionsRepository>();
var host = builder.Build();
host.Run();
