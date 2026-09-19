using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

#pragma warning disable CA1814 // Prefer jagged arrays over multidimensional

namespace Commissions.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class AddBrokerTiers : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "BrokerTiers",
                columns: table => new
                {
                    BrokerId = table.Column<string>(type: "nvarchar(450)", nullable: false),
                    Tier = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    Multiplier = table.Column<decimal>(type: "decimal(18,4)", precision: 18, scale: 4, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_BrokerTiers", x => x.BrokerId);
                });

            migrationBuilder.InsertData(
                table: "BrokerTiers",
                columns: new[] { "BrokerId", "Multiplier", "Tier" },
                values: new object[,]
                {
                    { "B100", 1.20m, "Gold" },
                    { "B200", 1.10m, "Silver" },
                    { "B300", 1.00m, "Bronze" },
                    { "B400", 0.95m, "Standard" }
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "BrokerTiers");
        }
    }
}
