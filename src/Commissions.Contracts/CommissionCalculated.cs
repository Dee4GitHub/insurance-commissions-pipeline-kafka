namespace Commissions.Contracts;

public record CommissionCalculated(
    string RowId, string BrokerId, string PolicyNumber,
    decimal CommissionAmount, string BatchId);
