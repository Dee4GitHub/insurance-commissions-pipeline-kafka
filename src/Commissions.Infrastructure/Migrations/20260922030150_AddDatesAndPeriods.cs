using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

#pragma warning disable CA1814 // Prefer jagged arrays over multidimensional

namespace Commissions.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class AddDatesAndPeriods : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            // Nullable first. The backfill below fills them, then they become NOT NULL.
            // Adding them NOT NULL directly would stamp 0001-01-01 on 450 existing rows.
            migrationBuilder.AddColumn<DateTimeOffset>(
                name: "EffectiveDate", table: "RawRows", type: "datetimeoffset", nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "PeriodKey", table: "RawRows", type: "nchar(7)",
                fixedLength: true, maxLength: 7, nullable: true);

            migrationBuilder.AddColumn<DateTimeOffset>(
                name: "EffectiveDate", table: "ProcessedRows", type: "datetimeoffset", nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "PeriodKey", table: "ProcessedRows", type: "nchar(7)",
                fixedLength: true, maxLength: 7, nullable: true);

            migrationBuilder.AddColumn<DateTimeOffset>(
                name: "RateAsOf", table: "ProcessedRows", type: "datetimeoffset", nullable: true);

            migrationBuilder.AddColumn<DateTimeOffset>(
                name: "FailedAt", table: "OutboxMessages", type: "datetimeoffset", nullable: true);

            migrationBuilder.AddColumn<bool>(
                name: "IsPeriodCoherent", table: "Batches", type: "bit",
                nullable: false, defaultValue: false);

            migrationBuilder.AddColumn<string>(
                name: "PeriodKey", table: "Batches", type: "nchar(7)",
                fixedLength: true, maxLength: 7, nullable: true);

            // BACKFILL. These rows were processed before effective dates existed, so there
            // is no true value to recover. Batch.UploadedAt is used: a real date from the
            // right era rather than a sentinel. IT IS NOT THE REAL EFFECTIVE DATE and any
            // reconciliation over historical rows should know that.
            migrationBuilder.Sql(@"
                UPDATE r SET r.EffectiveDate = b.UploadedAt
                FROM RawRows r JOIN Batches b ON b.BatchId = r.BatchId;

                UPDATE RawRows SET PeriodKey = FORMAT(EffectiveDate, 'yyyy-MM');

                UPDATE p SET p.EffectiveDate = b.UploadedAt, p.RateAsOf = b.UploadedAt
                FROM ProcessedRows p JOIN Batches b ON b.BatchId = p.BatchId;

                UPDATE ProcessedRows SET PeriodKey = FORMAT(EffectiveDate, 'yyyy-MM');

                UPDATE b SET b.PeriodKey = FORMAT(b.UploadedAt, 'yyyy-MM'),
                             b.IsPeriodCoherent = 1
                FROM Batches b;
            ");

            // Now they can be NOT NULL.
            migrationBuilder.AlterColumn<DateTimeOffset>(
                name: "EffectiveDate", table: "RawRows", type: "datetimeoffset", nullable: false);

            migrationBuilder.AlterColumn<string>(
                name: "PeriodKey", table: "RawRows", type: "nchar(7)",
                fixedLength: true, maxLength: 7, nullable: false);

            migrationBuilder.AlterColumn<DateTimeOffset>(
                name: "EffectiveDate", table: "ProcessedRows", type: "datetimeoffset", nullable: false);

            migrationBuilder.AlterColumn<string>(
                name: "PeriodKey", table: "ProcessedRows", type: "nchar(7)",
                fixedLength: true, maxLength: 7, nullable: false);

            migrationBuilder.AlterColumn<DateTimeOffset>(
                name: "RateAsOf", table: "ProcessedRows", type: "datetimeoffset", nullable: false);

            migrationBuilder.CreateTable(
                name: "Periods",
                columns: table => new
                {
                    PeriodKey = table.Column<string>(type: "nchar(7)", fixedLength: true, maxLength: 7, nullable: false),
                    Status = table.Column<byte>(type: "tinyint", nullable: false),
                    OpenedAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false),
                    ClosedAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Periods", x => x.PeriodKey);
                });
            migrationBuilder.InsertData(
                table: "Periods",
                columns: new[] { "PeriodKey", "ClosedAt", "OpenedAt", "Status" },
                values: new object[,]
                {
                    { "2026-07", new DateTimeOffset(new DateTime(2026, 8, 5, 0, 0, 0, 0, DateTimeKind.Unspecified), new TimeSpan(0, 0, 0, 0, 0)), new DateTimeOffset(new DateTime(2026, 7, 1, 0, 0, 0, 0, DateTimeKind.Unspecified), new TimeSpan(0, 0, 0, 0, 0)), (byte)1 },
                    { "2026-08", null, new DateTimeOffset(new DateTime(2026, 8, 1, 0, 0, 0, 0, DateTimeKind.Unspecified), new TimeSpan(0, 0, 0, 0, 0)), (byte)0 },
                    { "2026-09", null, new DateTimeOffset(new DateTime(2026, 9, 1, 0, 0, 0, 0, DateTimeKind.Unspecified), new TimeSpan(0, 0, 0, 0, 0)), (byte)0 },
                    { "2026-10", null, new DateTimeOffset(new DateTime(2026, 10, 1, 0, 0, 0, 0, DateTimeKind.Unspecified), new TimeSpan(0, 0, 0, 0, 0)), (byte)0 }
                });

            migrationBuilder.CreateIndex(
                name: "IX_ProcessedRows_PeriodKey",
                table: "ProcessedRows",
                column: "PeriodKey");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Periods");

            migrationBuilder.DropIndex(
                name: "IX_ProcessedRows_PeriodKey",
                table: "ProcessedRows");

            migrationBuilder.DropColumn(
                name: "EffectiveDate",
                table: "RawRows");

            migrationBuilder.DropColumn(
                name: "PeriodKey",
                table: "RawRows");

            migrationBuilder.DropColumn(
                name: "EffectiveDate",
                table: "ProcessedRows");

            migrationBuilder.DropColumn(
                name: "PeriodKey",
                table: "ProcessedRows");

            migrationBuilder.DropColumn(
                name: "RateAsOf",
                table: "ProcessedRows");

            migrationBuilder.DropColumn(
                name: "FailedAt",
                table: "OutboxMessages");

            migrationBuilder.DropColumn(
                name: "IsPeriodCoherent",
                table: "Batches");

            migrationBuilder.DropColumn(
                name: "PeriodKey",
                table: "Batches");
        }
    }
}
