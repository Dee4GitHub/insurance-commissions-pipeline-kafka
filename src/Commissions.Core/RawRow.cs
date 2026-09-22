namespace Commissions.Core;

public class RawRow
{
    public string RowId { get; set; } = default!;        // PK part 1
    public string BatchId { get; set; } = default!;      // PK part 2, FK to Batch
    public long LineNumber { get; set; }                  // where in the file
    public string RawLine { get; set; } = default!;      // the ORIGINAL text, unparsed
    public bool IsParseable { get; set; }
    public string? ParseError { get; set; }
    // parsed fields, null when IsParseable is false
    public string? BrokerId { get; set; }
    public string? PolicyNumber { get; set; }
    public decimal? PremiumAmount { get; set; }
    public decimal? CommissionRate { get; set; }
    public DateTimeOffset? EffectiveDate { get; set; }
    public string? PeriodKey { get; set; } = default!;
}