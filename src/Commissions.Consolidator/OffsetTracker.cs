namespace Commissions.Consolidator;

public class OffsetTracker
{
    private readonly Dictionary<TopicPartition, Offset> _lastGood = new();
    private readonly Dictionary<TopicPartition, Offset> _firstFailed = new();

    public void RecordSuccess(TopicPartition partition, Offset offset)
        => _lastGood[partition] = offset;

    public bool RecordFailure(TopicPartition partition, Offset offset)
    {
        if (_firstFailed.ContainsKey(partition))
        {
            return false;
        }

        _firstFailed[partition] = offset;
        return true;
    }

    public List<TopicPartitionOffset> CalculateCommitOffsets()
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

    public void ClearAfterCommit() => _lastGood.Clear();

    public void ForgetPartitions(IEnumerable<TopicPartition> partitions)
    {
        foreach (var partition in partitions)
        {
            _lastGood.Remove(partition);
            _firstFailed.Remove(partition);
        }
    }
}