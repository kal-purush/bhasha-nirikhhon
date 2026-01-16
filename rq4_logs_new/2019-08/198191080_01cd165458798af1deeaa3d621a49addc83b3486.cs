using Microsoft.EntityFrameworkCore.Migrations;

namespace NProducts.DAL.Migrations
{
    public partial class AddIsSoupForProducts : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<bool>(
                name: "IsSoup",
                table: "Products",
                nullable: false,
                defaultValue: false);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "IsSoup",
                table: "Products");
        }
    }
}