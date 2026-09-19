namespace Commissions.Contracts;

public record CommissionEnriched(
    string RowId, string BrokerId, string PolicyNumber,
    decimal PremiumAmount, decimal CommissionRate,
    string BrokerTier, decimal TierMultiplier, string BatchId);