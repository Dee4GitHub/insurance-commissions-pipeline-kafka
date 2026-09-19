var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<Worker>();
builder.Services.AddOptions<IngestorConfigOptions>()
    .Bind(builder.Configuration.GetSection(IngestorConfigOptions.SectionName))
    .Validate(o => !string.IsNullOrWhiteSpace(o.CsvFilePath), "CsvFilePath must be provided.")
    .Validate(o => o.FileChunkSize > 0, "FileChunkSize must be greater than zero.")
    .ValidateOnStart();

builder.Services.AddOptions<KafkaConfigOptions>()
    .Bind(builder.Configuration.GetSection(KafkaConfigOptions.SectionName))
    .Validate(o => !string.IsNullOrWhiteSpace(o.BootstrapServers), "BootstrapServers must be provided.")
    .Validate(o => !string.IsNullOrWhiteSpace(o.RawTopic), "RawTopic must be provided.")
    .ValidateOnStart();

builder.Services.AddSingleton<IMessagePublisher, KafkaMessagePublisher>();

builder.Services.AddSingleton<IProducer<string, string>>(sp => {
    var opts = sp.GetRequiredService<IOptions<KafkaConfigOptions>>().Value;
    return new ProducerBuilder<string, string>(
        new ProducerConfig { BootstrapServers = opts.BootstrapServers }).Build();
});

builder.Services.AddDbContext<CommissionsDBContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("CommissionsDb")));

var host = builder.Build();
host.Run();
