namespace Commissions.Core;

public record BatchSummary(
    string BatchId,
    string AgentEmail,
    int RowCount,
    decimal TotalCommission,
    DateTimeOffset CompletedAt,
    string? PeriodKey,
    bool IsPeriodCoherent);