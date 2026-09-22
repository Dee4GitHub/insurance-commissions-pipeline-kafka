using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Commissions.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class MakeRawRowDatesNullable : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterColumn<string>(
                name: "PeriodKey",
                table: "RawRows",
                type: "nchar(7)",
                fixedLength: true,
                maxLength: 7,
                nullable: true,
                oldClrType: typeof(string),
                oldType: "nchar(7)",
                oldFixedLength: true,
                oldMaxLength: 7);

            migrationBuilder.AlterColumn<DateTimeOffset>(
                name: "EffectiveDate",
                table: "RawRows",
                type: "datetimeoffset",
                nullable: true,
                oldClrType: typeof(DateTimeOffset),
                oldType: "datetimeoffset");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterColumn<string>(
                name: "PeriodKey",
                table: "RawRows",
                type: "nchar(7)",
                fixedLength: true,
                maxLength: 7,
                nullable: false,
                defaultValue: "",
                oldClrType: typeof(string),
                oldType: "nchar(7)",
                oldFixedLength: true,
                oldMaxLength: 7,
                oldNullable: true);

            migrationBuilder.AlterColumn<DateTimeOffset>(
                name: "EffectiveDate",
                table: "RawRows",
                type: "datetimeoffset",
                nullable: false,
                defaultValue: new DateTimeOffset(new DateTime(1, 1, 1, 0, 0, 0, 0, DateTimeKind.Unspecified), new TimeSpan(0, 0, 0, 0, 0)),
                oldClrType: typeof(DateTimeOffset),
                oldType: "datetimeoffset",
                oldNullable: true);
        }
    }
}
