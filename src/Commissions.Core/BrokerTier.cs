namespace Commissions.Core;

public class BrokerTier
{
    public string BrokerId { get; set; } = default!;   // PK
    public string Tier { get; set; } = default!;       // Gold / Silver / Bronze
    public decimal Multiplier { get; set; }
}