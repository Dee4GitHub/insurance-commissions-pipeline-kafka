namespace Commissions.Notifier;

public class Worker(
    ILogger<Worker> logger,
    IServiceScopeFactory serviceScopeFactory,
    IOptions<NotifierConfigOptions> options) : BackgroundService
{
    private readonly string _instanceId = $"{Environment.MachineName}:{Environment.ProcessId}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = options.Value;
        var drainInterval = TimeSpan.FromSeconds(config.DrainIntervalSeconds);
        var consecutiveFailures = 0;
        var backstopInterval = TimeSpan.FromSeconds(config.BackstopIntervalSeconds);
        var lastBackstop = DateTimeOffset.MinValue;

        logger.LogInformation("Notifier {InstanceId} started", _instanceId);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var claimed = await DrainOnceAsync(config, stoppingToken);
                consecutiveFailures = 0;

                // THIS PLACEMENT IS THE M10 RULE: the backstop only runs on an iteration where
                // the drain just succeeded, so it is skipped entirely while the drain is
                // failing. Do not move it above the drain.
                // lastBackstop is stamped BEFORE running, so a failing backstop waits a full
                // interval rather than retrying on every loop.
                if (DateTimeOffset.UtcNow - lastBackstop >= backstopInterval)
                {
                    lastBackstop = DateTimeOffset.UtcNow;
                    await BackstopOnceAsync(config, stoppingToken);
                }

                // Nothing waiting: sleep. Work found: go straight round again, because
                // there may be more.
                if (claimed == 0)
                {
                    await Task.Delay(drainInterval, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                consecutiveFailures++;
                logger.LogError(ex, "Drain failed ({Count} in a row)", consecutiveFailures);

                try
                {
                    await Task.Delay(drainInterval, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        logger.LogInformation("Notifier {InstanceId} stopping", _instanceId);
    }

    private async Task<int> DrainOnceAsync(NotifierConfigOptions config, CancellationToken ct)
    {
        using var scope = serviceScopeFactory.CreateScope();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();
        var sender = scope.ServiceProvider.GetRequiredService<INotificationSender>();

        var claimed = await outbox.ClaimBatchAsync(config.BatchSize, config.LeaseSeconds, _instanceId, ct);

        foreach (var message in claimed)
        {
            if (ct.IsCancellationRequested)
            {
                break;
            }

            await SendOneAsync(outbox, sender, message, config, ct);
        }

        return claimed.Count;
    }

    private async Task SendOneAsync(
        IOutboxRepository outbox,
        INotificationSender sender,
        OutboxMessage message,
        NotifierConfigOptions config,
        CancellationToken ct)
    {
        var suppression = await ShouldSuppressAsync(outbox, message, ct);

        if (suppression is not null)
        {
            await outbox.MarkSuppressedAsync(message.OutboxId, suppression, ct);

            logger.LogInformation(
                "Notification for batch {BatchId} SUPPRESSED: {Reason}. This is a business " +
                "decision, not a failure, and it will not be retried.",
                message.AggregateId, suppression);

            return;
        }
        try
        {
            await sender.SendAsync(message.DedupeKey, message.Payload, ct);

            await outbox.MarkSentAsync(message.OutboxId, message.AggregateId, ct);

            logger.LogInformation(
                "Sent notification for batch {BatchId} (attempt {Attempt})",
                message.AggregateId, message.AttemptCount);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // The lease expires on its own.
        }
        catch (Exception ex)
        {
            if (message.AttemptCount >= config.MaxAttempts)
            {
                await outbox.MarkDeadAsync(message.OutboxId, ex.Message, CancellationToken.None);

                logger.LogError(ex,
                    "Batch {BatchId} DEAD after {Attempts} attempts. It will not be retried.",
                    message.AggregateId, message.AttemptCount);
            }
            else
            {
                var backoff = BackoffSeconds(message.AttemptCount, config);
                await outbox.MarkFailedAsync(
                    message.OutboxId, ex.Message, backoff, CancellationToken.None);

                logger.LogWarning(ex,
                    "Batch {BatchId} failed (attempt {Attempt}), retrying in {Backoff}s",
                    message.AggregateId, message.AttemptCount, backoff);
            }
        }
    }

    // Exponential, capped, with jitter. The jitter matters: without it every
    // message that failed during the same outage retries in the same instant, forever.
    private static int BackoffSeconds(int attemptCount, NotifierConfigOptions config)
    {
        var exponential = config.BaseBackoffSeconds * Math.Pow(2, attemptCount - 1);
        var capped = Math.Min(exponential, config.MaxBackoffSeconds);
        var jitter = Random.Shared.NextDouble() * 0.2 * capped;

        return (int)(capped + jitter);
    }

    // Returns a reason to suppress, or null to send. A suppressed message is a BUSINESS
    // outcome, not an error - it must never be retried, and it must read differently in
    // the logs from something that broke.
    private static async Task<string?> ShouldSuppressAsync(
        IOutboxRepository outbox, OutboxMessage message, CancellationToken ct)
    {
        // Checked first: a pure field read, no database call needed to disqualify.
        if (!message.IsPeriodCoherent)
        {
            return "the batch spans more than one accounting period";
        }

        if (message.PeriodKey is null)
        {
            // Written before periods existed. Nothing to check against.
            return null;
        }

        if (!await outbox.IsPeriodOpenAsync(message.PeriodKey, ct))
        {
            return $"period {message.PeriodKey} is closed";
        }

        return null;
    }
    // Finds completed batches that have no outbox row and writes one, using the same
    // transaction path and the same message builder as the Consolidator. In a healthy
    // system this finds nothing, so every batch it finds is logged as a WARNING.
    private async Task BackstopOnceAsync(NotifierConfigOptions config, CancellationToken ct)
    {
        IReadOnlyList<string> batchIds;

        using (var scope = serviceScopeFactory.CreateScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();
            batchIds = await outbox.FindUnenqueuedCompleteBatchesAsync(config.BatchSize, ct);
        }

        foreach (var batchId in batchIds)
        {
            if (ct.IsCancellationRequested)
            {
                break;
            }

            // A NEW SCOPE PER BATCH. If oneys tracked in that
            // DbContext as Added; a shared context would try to insert it again on the next
            // batch's SaveChanges and fail
            using var scope = serviceScopeFactory.CreateScope();
            var commissions = scope.ServiceProvider.GetRequiredService<ICommissionRepository>();

            try
            {
                await using var tx = await commissions.BeginTransactionAsync(ct);

                var summary = await commissions.GetBatchSummaryAsync(batchId, tx, ct);
                await commissions.AddOutboxMessageAsync(BatchNotification.From(summary), tx, ct);

                await tx.CommitAsync(ct);

                logger.LogWarning(
                    "BACKSTOP enqueued batch {BatchId}. It was Complete with " +
                    "no outbox row - either it predates the outbox, or some path completed it " +
                    "without writing one.",
                    batchId);
            }
            catch (DbUpdateException ex) when (IsUniqueViolation(ex))
            {
                // Another Notifier instance enqueued it first. That is the correct outcome.
                logger.LogInformation(
                    "Batch {BatchId} was enqueued by another instance first", batchId);
            }
        }
    }

    // 2601 = duplicate key in a unique INDEunique or primary
    // key CONSTRAINT (sys.messages, language_id 1033). DedupeKey is a unique index, so
    // 2601 is the one expected here; 2627 imade a constraint.
    private static bool IsUniqueViolation(DbUpdateException ex) =>
        ex.InnerException is SqlException { Number: 2601 or 2627 };
}