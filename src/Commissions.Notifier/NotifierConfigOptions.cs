namespace Commissions.Notifier;

public class NotifierConfigOptions
{
    public const string SectionName = "Notifier";

    public int DrainIntervalSeconds { get; set; }
    public int BackstopIntervalSeconds { get; set; }
    public int BatchSize { get; set; }
    public int LeaseSeconds { get; set; }
    public int MaxAttempts { get; set; }
    public int BaseBackoffSeconds { get; set; }
    public int MaxBackoffSeconds { get; set; }

    // TEST ONLY. When true, the process kills itself straight after a successful send and
    // before the outbox row is marked Sent. Never set in appsettings - pass it on the
    // command line for the kill/restart experiment.
    public bool CrashAfterSend { get; set; }
}