namespace Commissions.Core;

public interface IMessagePublisher
{
    Task PublishAsync<T>(string topic, string key, T message, CancellationToken ct);
}