namespace Commissions.Core;
public static class CommissionCalculator
{
    public static decimal CalculateCommission(decimal premiumAmount, decimal commissionRate, decimal multiplier)
    => Math.Round(premiumAmount * commissionRate * multiplier, 2, MidpointRounding.ToEven);
}