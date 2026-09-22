namespace Commissions.Infrastructure;

public class CommissionsDBContext : DbContext
{
    public CommissionsDBContext(DbContextOptions<CommissionsDBContext> options) : base(options)
    {
    }

    public DbSet<RawRow> RawRows { get; set; } = default!;
    public DbSet<Batch> Batches { get; set; } = default!;
    public DbSet<ProcessedRow> ProcessedRows { get; set; } = default!;
    public DbSet<BrokerTier> BrokerTiers { get; set; } = default!;
    public DbSet<OutboxMessage> OutboxMessages { get; set; } = default!;
    public DbSet<Period> Periods { get; set; } = default!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Batch>()
            .HasKey(b => b.BatchId);

        modelBuilder.Entity<RawRow>()
            .HasKey(rr => new { rr.RowId, rr.BatchId });

        modelBuilder.Entity<RawRow>()
            .HasOne<Batch>()
            .WithMany()
            .HasForeignKey(rr => rr.BatchId)
            .OnDelete(DeleteBehavior.Restrict);

        modelBuilder.Entity<RawRow>()
            .Property(rr => rr.PremiumAmount)
            .HasPrecision(18, 2);

        modelBuilder.Entity<RawRow>()
            .Property(rr => rr.CommissionRate)
            .HasPrecision(18, 6);

        modelBuilder.Entity<ProcessedRow>()
            .HasKey(pr => new { pr.RowId, pr.BatchId });

        modelBuilder.Entity<ProcessedRow>()
            .HasIndex(pr => pr.BatchId);

        modelBuilder.Entity<ProcessedRow>()
            .Property(pr => pr.CommissionAmount)
            .HasPrecision(18, 4);

        modelBuilder.Entity<BrokerTier>()
            .HasKey(b => b.BrokerId);

        modelBuilder.Entity<BrokerTier>()
            .Property(b => b.Multiplier)
            .HasPrecision(18, 4);

        modelBuilder.Entity<BrokerTier>().HasData(
            new BrokerTier { BrokerId = "B100", Tier = "Gold", Multiplier = 1.20m },
            new BrokerTier { BrokerId = "B200", Tier = "Silver", Multiplier = 1.10m },
            new BrokerTier { BrokerId = "B300", Tier = "Bronze", Multiplier = 1.00m },
            new BrokerTier { BrokerId = "B400", Tier = "Standard", Multiplier = 0.95m }
            );

        modelBuilder.Entity<OutboxMessage>()
            .HasKey(o => o.OutboxId);

        modelBuilder.Entity<OutboxMessage>()
            .HasIndex(o => o.DedupeKey)
            .IsUnique();

        modelBuilder.Entity<OutboxMessage>()
            .HasIndex(o => new { o.Status, o.AvailableAt });

        modelBuilder.Entity<OutboxMessage>()
            .HasIndex(o => o.AggregateId);

        modelBuilder.Entity<OutboxMessage>()
            .Property(o => o.MessageType)
            .HasMaxLength(100);

        modelBuilder.Entity<OutboxMessage>()
            .Property(o => o.AggregateId)
            .HasMaxLength(100);

        modelBuilder.Entity<OutboxMessage>()
            .Property(o => o.DedupeKey)
            .HasMaxLength(200);

        modelBuilder.Entity<OutboxMessage>()
            .Property(o => o.PeriodKey)
            .HasMaxLength(7);

        modelBuilder.Entity<OutboxMessage>()
            .Property(o => o.LockedBy)
            .HasMaxLength(100);

        modelBuilder.Entity<OutboxMessage>()
            .Property(o => o.LastError)
            .HasMaxLength(2000);

        modelBuilder.Entity<Period>()
            .HasKey(p => p.PeriodKey);

        modelBuilder.Entity<Period>()
            .Property(p => p.PeriodKey)
            .HasMaxLength(7)
            .IsFixedLength();

        modelBuilder.Entity<Period>().HasData(
            new Period
            {
                PeriodKey = "2026-07",
                Status = PeriodStatus.Closed,
                OpenedAt = new DateTimeOffset(2026, 7, 1, 0, 0, 0, TimeSpan.Zero),
                ClosedAt = new DateTimeOffset(2026, 8, 5, 0, 0, 0, TimeSpan.Zero)
            },
            new Period
            {
                PeriodKey = "2026-08",
                Status = PeriodStatus.Open,
                OpenedAt = new DateTimeOffset(2026, 8, 1, 0, 0, 0, TimeSpan.Zero)
            },
            new Period
            {
                PeriodKey = "2026-09",
                Status = PeriodStatus.Open,
                OpenedAt = new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero)
            },
            new Period
            {
                PeriodKey = "2026-10",
                Status = PeriodStatus.Open,
                OpenedAt = new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero)
            }
        );

        modelBuilder.Entity<RawRow>()
            .Property(rr => rr.PeriodKey)
            .HasMaxLength(7)
            .IsFixedLength();

        modelBuilder.Entity<ProcessedRow>()
            .Property(pr => pr.PeriodKey)
            .HasMaxLength(7)
            .IsFixedLength();

        modelBuilder.Entity<ProcessedRow>()
            .HasIndex(pr => pr.PeriodKey);

        modelBuilder.Entity<Batch>()
            .Property(b => b.PeriodKey)
            .HasMaxLength(7)
            .IsFixedLength();
    }

}