namespace Commissions.Infrastructure;

public class BrokerTierLookup(CommissionsDBContext db):IBrokerTierLookup
{
    public async Task<(string Tier, decimal Multiplier)> GetTierAsync(
        string brokerId, CancellationToken ct){       
        var tier = await db.BrokerTiers
            .AsNoTracking()
            .FirstOrDefaultAsync(bt => bt.BrokerId == brokerId, ct);

        if (tier is null)
        {
            throw new InvalidOperationException($"No tier configured for broker {brokerId}");
        }

        return (tier.Tier, tier.Multiplier);
     }
}