namespace Commissions.Consolidator;

public class ConsolidatorConfigOptions
{
    public const string SectionName = "Consolidation";
    public int BatchSize { get; set; }
    public int FlushIntervalSeconds { get; set; }
}