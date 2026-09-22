namespace Commissions.Core;

public class OutboxMessage
{
    public long OutboxId { get; set; }
    public string MessageType { get; set; } = default!;
    public string AggregateId { get; set; } = default!;
    public string DedupeKey { get; set; } = default!;
    public string Payload { get; set; } = default!;
    public string? PeriodKey { get; set; }
    public DateTimeOffset OccurredAt { get; set; }
    public DateTimeOffset AvailableAt { get; set; }
    public OutboxStatus Status { get; set; }
    public int AttemptCount { get; set; }
    public string? LastError { get; set; }
    public DateTimeOffset? LockedUntil { get; set; }
    public string? LockedBy { get; set; }
    public DateTimeOffset? SentAt { get; set; }
    public DateTimeOffset? FailedAt { get; set; }
    public bool IsPeriodCoherent { get; set; } = true;
}