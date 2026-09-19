using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Commissions.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class AddStagingTables : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Batches",
                columns: table => new
                {
                    BatchId = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    FileName = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    AgencyId = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    AgentEmail = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    UploadedAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false),
                    ExpectedRowCount = table.Column<int>(type: "int", nullable: false),
                    RejectedRowCount = table.Column<int>(type: "int", nullable: false),
                    Status = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    CompletedAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: true),
                    NotifiedAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Batches", x => x.BatchId);
                });

            migrationBuilder.CreateTable(
                name: "RawRows",
                columns: table => new
                {
                    RowId = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    BatchId = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    LineNumber = table.Column<int>(type: "int", nullable: false),
                    RawLine = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    IsParseable = table.Column<bool>(type: "bit", nullable: false),
                    ParseError = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    BrokerId = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    PolicyNumber = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    PremiumAmount = table.Column<decimal>(type: "decimal(18,2)", nullable: true),
                    CommissionRate = table.Column<decimal>(type: "decimal(18,2)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_RawRows", x => new { x.RowId, x.BatchId });
                    table.ForeignKey(
                        name: "FK_RawRows_Batches_BatchId",
                        column: x => x.BatchId,
                        principalTable: "Batches",
                        principalColumn: "BatchId",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateIndex(
                name: "IX_ProcessedRows_BatchId",
                table: "ProcessedRows",
                column: "BatchId");

            migrationBuilder.CreateIndex(
                name: "IX_RawRows_BatchId",
                table: "RawRows",
                column: "BatchId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "RawRows");

            migrationBuilder.DropTable(
                name: "Batches");

            migrationBuilder.DropIndex(
                name: "IX_ProcessedRows_BatchId",
                table: "ProcessedRows");
        }
    }
}
