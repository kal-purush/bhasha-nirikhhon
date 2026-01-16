using Microsoft.EntityFrameworkCore.Migrations;

namespace CISS411_Project.Migrations
{
    public partial class M5 : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "SeatsAvailable",
                table: "Swimmers",
                nullable: false,
                defaultValue: 0);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "SeatsAvailable",
                table: "Swimmers");
        }
    }
}