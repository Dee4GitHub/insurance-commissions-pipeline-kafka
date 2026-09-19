namespace Commissions.Core;
public class Batch
{
    public string BatchId { get; set; } = default!;      // PK, a Guid string
    public string FileName { get; set; } = default!;
    public string AgencyId { get; set; } = default!;     // who uploaded it
    public string AgentEmail { get; set; } = default!;   // stage 5 needs this
    public DateTimeOffset UploadedAt { get; set; }
    public int ExpectedRowCount { get; set; }            // <- consolidation compares to this
    public int RejectedRowCount { get; set; }            // rows that would not parse
    public string Status { get; set; } = default!;       // Loading|Processing|Complete|Failed
    public DateTimeOffset? CompletedAt { get; set; }
    public DateTimeOffset? NotifiedAt { get; set; }      // <- the email idempotency marker
}