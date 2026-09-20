public class OffsetTrackerTests
{
    private static TopicPartition P0 => new("commissions.calculated", new Partition(0));
    private static TopicPartition P1 => new("commissions.calculated", new Partition(1));

    [Fact]
    public void CommitsOnePastTheLastGoodOffset()
    {
        var tracker = new OffsetTracker();

        tracker.RecordSuccess(P0, new Offset(10));
        tracker.RecordSuccess(P0, new Offset(11));

        var result = tracker.CalculateCommitOffsets();

        Assert.Single(result);
        Assert.Equal(12, result[0].Offset.Value);
    }

    [Fact]
    public void DoesNotCommitPastAFailureInTheMiddleOfTheBatch()
    {
        var tracker = new OffsetTracker();

        tracker.RecordSuccess(P0, new Offset(10));
        tracker.RecordFailure(P0, new Offset(11));
        tracker.RecordSuccess(P0, new Offset(12));

        var result = tracker.CalculateCommitOffsets();

        Assert.Equal(11, result[0].Offset.Value);
    }

    [Fact]
    public void KeepsTheFirstFailureNotTheLatest()
    {
        var tracker = new OffsetTracker();

        tracker.RecordSuccess(P0, new Offset(10));
        tracker.RecordFailure(P0, new Offset(11));
        tracker.RecordSuccess(P0, new Offset(12));
        tracker.RecordFailure(P0, new Offset(13));
        tracker.RecordSuccess(P0, new Offset(14));

        var result = tracker.CalculateCommitOffsets();

        Assert.Equal(11, result[0].Offset.Value);
    }

    [Fact]
    public void ReportsOnlyTheFirstFailureOnAPartition()
    {
        var tracker = new OffsetTracker();

        Assert.True(tracker.RecordFailure(P0, new Offset(11)));
        Assert.False(tracker.RecordFailure(P0, new Offset(13)));
    }

    [Fact]
    public void TracksPartitionsIndependently()
    {
        var tracker = new OffsetTracker();

        tracker.RecordSuccess(P0, new Offset(10));
        tracker.RecordFailure(P0, new Offset(11));
        tracker.RecordSuccess(P0, new Offset(12));
        tracker.RecordSuccess(P1, new Offset(50));

        var result = tracker.CalculateCommitOffsets();

        Assert.Equal(11, result.Single(r => r.Partition.Value == 0).Offset.Value);
        Assert.Equal(51, result.Single(r => r.Partition.Value == 1).Offset.Value);
    }

    [Fact]
    public void OmitsAPartitionThatHasOnlyFailures()
    {
        var tracker = new OffsetTracker();

        tracker.RecordFailure(P0, new Offset(11));

        var result = tracker.CalculateCommitOffsets();

        Assert.Empty(result);
    }

    [Fact]
    public void KeepsBlockingAPartitionAfterACommit()
    {
        var tracker = new OffsetTracker();

        tracker.RecordSuccess(P0, new Offset(10));
        tracker.RecordFailure(P0, new Offset(11));
        tracker.CalculateCommitOffsets();
        tracker.ClearAfterCommit();

        tracker.RecordSuccess(P0, new Offset(12));

        var result = tracker.CalculateCommitOffsets();

        Assert.Equal(11, result[0].Offset.Value);
    }

    [Fact]
    public void DoesNotCommitBackwardsAfterAPartitionIsRevoked()
    {
        var tracker = new OffsetTracker();

        tracker.RecordFailure(P0, new Offset(50));
        tracker.ForgetPartitions([P0]);

        tracker.RecordSuccess(P0, new Offset(900));

        var result = tracker.CalculateCommitOffsets();

        Assert.Equal(901, result[0].Offset.Value);
    }

    [Fact]
    public void ForgettingOnePartitionLeavesTheOthers()
    {
        var tracker = new OffsetTracker();

        tracker.RecordSuccess(P0, new Offset(10));
        tracker.RecordFailure(P1, new Offset(50));
        tracker.RecordSuccess(P1, new Offset(51));

        tracker.ForgetPartitions([P0]);

        var result = tracker.CalculateCommitOffsets();

        Assert.Equal(50, result.Single().Offset.Value);
    }
}
