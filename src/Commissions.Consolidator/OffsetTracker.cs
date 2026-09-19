namespace Commissions.Consolidator;

// Called from the worker loop and from the Kafka partitions-revoked handler.
// Confluent.Kafka currently dispatches that handler from inside Consume(), on the
// same thread as the loop, so these would be safe without the lock - but that is a
// property of the client's dispatch model, not of this class. The lock makes the
// safety explicit and costs nothing at one call per message.
public class OffsetTracker
{
    private readonly Lock _gate = new();
    private readonly Dictionary<TopicPartition, Offset> _lastGood = new();
    private readonly Dictionary<TopicPartition, Offset> _firstFailed = new();

    public void RecordSuccess(TopicPartition partition, Offset offset)
    {
        lock (_gate)
        {
            _lastGood[partition] = offset;
        }
    }

    public bool RecordFailure(TopicPartition partition, Offset offset)
    {
        lock (_gate)
        {
            if (_firstFailed.ContainsKey(partition))
            {
                return false;
            }

            _firstFailed[partition] = offset;
            return true;
        }
    }

    public List<TopicPartitionOffset> CalculateCommitOffsets()
    {
        lock (_gate)
        {
            var toCommit = new List<TopicPartitionOffset>();

            foreach (var (partition, offset) in _lastGood)
            {
                var safe = offset + 1;

                if (_firstFailed.TryGetValue(partition, out var failed) && failed < safe)
                {
                    safe = failed;
                }

                toCommit.Add(new TopicPartitionOffset(partition, safe));
            }

            return toCommit;
        }
    }

    public void ClearAfterCommit()
    {
        lock (_gate)
        {
            _lastGood.Clear();
        }
    }

    public void ForgetPartitions(IEnumerable<TopicPartition> partitions)
    {
        lock (_gate)
        {
            foreach (var partition in partitions)
            {
                _lastGood.Remove(partition);
                _firstFailed.Remove(partition);
            }
        }
    }
}