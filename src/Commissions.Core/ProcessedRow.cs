namespace Commissions.Core;
public class ProcessedRow
{
    public string RowId { get; set; } = default!;      // part of the PK
    public string BatchId { get; set; } = default!;    // part of the PK
    public string BrokerId { get; set; } = default!;
    public decimal CommissionAmount { get; set; }
    public DateTimeOffset ProcessedAt { get; set; }
}