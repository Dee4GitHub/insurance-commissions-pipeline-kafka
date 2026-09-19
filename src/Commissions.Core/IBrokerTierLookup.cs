namespace Commissions.Core;
public interface IBrokerTierLookup 
{
    Task<(string Tier, decimal Multiplier)> GetTierAsync(string brokerId, CancellationToken ct);
}