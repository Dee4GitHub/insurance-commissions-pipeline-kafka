namespace Commissions.Ingestor;
public class Worker(
    ILogger<Worker> logger,
    IServiceScopeFactory serviceScopeFactory,
    IOptions<IngestorConfigOptions> ingestorConfigOptions,
    IOptions<KafkaConfigOptions> kafkaConfigOptions,
    IMessagePublisher publisher,
    IHostApplicationLifetime lifetime): BackgroundService
{
    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        using var scope = serviceScopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<CommissionsDBContext>();
        var chunkSize = ingestorConfigOptions.Value.FileChunkSize;
        var csvPath = ingestorConfigOptions.Value.CsvFilePath;
        var canConnect = await db.Database.CanConnectAsync(stoppingToken);

        if(canConnect)
        { 
            
            logger.LogInformation("Successfully connected to the database.");
            using var fileStream = new FileStream(csvPath, FileMode.Open, FileAccess.Read);
            // Process the CSV file
            using var reader = new StreamReader(fileStream);
            string? line;
            var lineNumber = 0L;                
            var batchId = Guid.NewGuid().ToString();
            var parseableRowCount = 0L;
            var rejectedRowCount = 0L;
            var batch = new Batch
            {
                BatchId = batchId,
                FileName = csvPath,
                AgencyId = "AGENCY-001",
                AgentEmail = "agent@example.com",
                UploadedAt = DateTimeOffset.UtcNow,
                Status = "Loading"
            };
            await db.Batches.AddAsync(batch);
            await db.SaveChangesAsync(stoppingToken);
            var rawRows = new List<RawRow>();
            while ((line = await reader.ReadLineAsync(stoppingToken)) != null)
            {
                // Process each line of the CSV file
                lineNumber++;
                if (lineNumber == 1) // Skip header line
                    continue;

                var rawLine = RawRowParser.Parse(line, lineNumber, batchId);
                if (rawLine.IsParseable)
                {
                    parseableRowCount++;
                }
                else
                {
                    rejectedRowCount++;
                    logger.LogWarning($"Line {lineNumber} is not parseable: {rawLine.ParseError}");
                }
                rawRows.Add(rawLine);

                if (rawRows.Count >= chunkSize)
                {
                    await db.RawRows.AddRangeAsync(rawRows, stoppingToken);
                    await db.SaveChangesAsync(stoppingToken);
                    rawRows.Clear();
                }               
            }
            if (rawRows.Count > 0)
            {
                await db.RawRows.AddRangeAsync(rawRows, stoppingToken);
            }
            batch.Status = "Processing";
            batch.ExpectedRowCount = (int)parseableRowCount;
            batch.RejectedRowCount = (int)rejectedRowCount;
            await db.SaveChangesAsync(stoppingToken);

            if((int)parseableRowCount > 0 ){
                await PublishBatchAsync(db, batch.BatchId, chunkSize, stoppingToken);
            }
        }
        else
        {
            logger.LogError("Failed to connect to the database.");
        }
        lifetime.StopApplication();
    }

    private async Task PublishBatchAsync(
        CommissionsDBContext db, string batchId, int chunkSize, CancellationToken ct)
    {
        var published = 0;
        var skip = 0;
        var topic = kafkaConfigOptions.Value.RawTopic;

        while (true)
        {
            var page = await db.RawRows
                .AsNoTracking()
                .Where(r => r.BatchId == batchId && r.IsParseable)
                .OrderBy(r => r.RowId)
                .Skip(skip)
                .Take(chunkSize)
                .ToListAsync(ct);

            if (page.Count == 0) break;

            foreach (var row in page)
            {
                var message = new CommissionRaw(
                    row.RowId, row.BrokerId!, row.PolicyNumber!,
                    row.PremiumAmount!.Value, row.CommissionRate!.Value, batchId);

                await publisher.PublishAsync(topic, row.RowId!, message, ct);
                published++;
            }

            skip += page.Count;
        }

        logger.LogInformation("Published {Count} messages for batch {BatchId}", published, batchId);
    }    
}
