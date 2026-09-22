namespace Commissions.Contracts;

public record CommissionRaw(
    string RowId, string BrokerId, string PolicyNumber,
    decimal PremiumAmount, decimal CommissionRate, string BatchId,
    DateTimeOffset EffectiveDate, string PeriodKey);

