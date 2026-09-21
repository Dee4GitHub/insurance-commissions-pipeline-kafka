namespace Commissions.Core;

public enum OutboxStatus : byte
{
    Pending = 0,
    Sent = 1,
    Dead = 2,
    Suppressed = 3
}