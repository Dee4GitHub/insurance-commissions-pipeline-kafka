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

        logger.LogInformation("Notifier {InstanceId} started", _instanceId);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var claimed = await DrainOnceAsync(config, stoppingToken);
                consecutiveFailures = 0;

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

    // Exponential, capped, with jitter. THEthout it every
    // message that failed during the same outage retries in the same instant, forever.
    private static int BackoffSeconds(int attemptCount, NotifierConfigOptions config)
    {
        var exponential = config.BaseBackoffSeconds * Math.Pow(2, attemptCount - 1);
        var capped = Math.Min(exponential, config.MaxBackoffSeconds);
        var jitter = Random.Shared.NextDouble() * 0.2 * capped;

        return (int)(capped + jitter);
    }
}