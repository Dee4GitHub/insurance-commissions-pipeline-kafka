namespace Commissions.Core;

public class Period
{
    public string PeriodKey { get; set; } = default!;   // 'yyyy-MM'
    public PeriodStatus Status { get; set; }
    public DateTimeOffset OpenedAt { get; set; }
    public DateTimeOffset? ClosedAt { get; set; }
}