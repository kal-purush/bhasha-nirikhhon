using Microsoft.EntityFrameworkCore.Migrations;

namespace TRPR.Data.TRPRMigrations
{
    public partial class filetypenull : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterColumn<int>(
                name: "TypeID",
                schema: "TRPR",
                table: "Files",
                nullable: true,
                oldClrType: typeof(int));
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterColumn<int>(
                name: "TypeID",
                schema: "TRPR",
                table: "Files",
                nullable: false,
                oldClrType: typeof(int),
                oldNullable: true);
        }
    }
}