
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();
builder.Services.AddOptions<KafkaConfigOptions>()
    .Bind(builder.Configuration.GetSection(KafkaConfigOptions.SectionName))
    .Validate(o => !string.IsNullOrWhiteSpace(o.BootstrapServers), "BootstrapServers must be provided.")
    .Validate(o => !string.IsNullOrWhiteSpace(o.EnrichedTopic), "EnrichedTopic must be provided.")
    .Validate(o => !string.IsNullOrWhiteSpace(o.CalculatedTopic), "CalculatedTopic must be provided.")
    .Validate(o => !string.IsNullOrWhiteSpace(o.ConsumerGroupId), "ConsumerGroupId must be provided.")
    .ValidateOnStart();
builder.Services.AddSingleton<IConsumer<string, string>>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<KafkaConfigOptions>>().Value;
    return new ConsumerBuilder<string, string>(new ConsumerConfig
    {
        BootstrapServers = opts.BootstrapServers,
        GroupId = opts.ConsumerGroupId,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false
    }).Build();
});

builder.Services.AddSingleton<IProducer<string, string>>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<KafkaConfigOptions>>().Value;
    return new ProducerBuilder<string, string>(
        new ProducerConfig { BootstrapServers = opts.BootstrapServers }).Build();
});
builder.Services.AddSingleton<IMessagePublisher, KafkaMessagePublisher>();
var host = builder.Build();
host.Run();
