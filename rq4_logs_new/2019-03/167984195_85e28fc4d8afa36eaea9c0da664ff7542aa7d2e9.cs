using Microsoft.EntityFrameworkCore.Migrations;

namespace TRPR.Data.TRPRMigrations
{
    public partial class ResActive : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterColumn<string>(
                name: "ResTitle",
                schema: "TRPR",
                table: "Researchers",
                maxLength: 10,
                nullable: false,
                oldClrType: typeof(string),
                oldMaxLength: 10,
                oldNullable: true);

            migrationBuilder.AlterColumn<string>(
                name: "ResBio",
                schema: "TRPR",
                table: "Researchers",
                maxLength: 500,
                nullable: true,
                oldClrType: typeof(string),
                oldMaxLength: 500);

            migrationBuilder.AddColumn<bool>(
                name: "Active",
                schema: "TRPR",
                table: "Researchers",
                nullable: false,
                defaultValue: false);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "Active",
                schema: "TRPR",
                table: "Researchers");

            migrationBuilder.AlterColumn<string>(
                name: "ResTitle",
                schema: "TRPR",
                table: "Researchers",
                maxLength: 10,
                nullable: true,
                oldClrType: typeof(string),
                oldMaxLength: 10);

            migrationBuilder.AlterColumn<string>(
                name: "ResBio",
                schema: "TRPR",
                table: "Researchers",
                maxLength: 500,
                nullable: false,
                oldClrType: typeof(string),
                oldMaxLength: 500,
                oldNullable: true);
        }
    }
}