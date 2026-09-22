using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Commissions.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class AddIsPeriodCoherentToOutbox : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<bool>(
                name: "IsPeriodCoherent",
                table: "OutboxMessages",
                type: "bit",
                nullable: false,
                defaultValue: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "IsPeriodCoherent",
                table: "OutboxMessages");
        }
    }
}
