using Microsoft.EntityFrameworkCore.Migrations;

namespace VillageDrill.Migrations
{
    public partial class UpdateItem : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_Item_Measures_MeasuresMeasureID",
                table: "Item");

            migrationBuilder.RenameColumn(
                name: "MeasuresMeasureID",
                table: "Item",
                newName: "MeasureID");

            migrationBuilder.RenameIndex(
                name: "IX_Item_MeasuresMeasureID",
                table: "Item",
                newName: "IX_Item_MeasureID");

            migrationBuilder.AddForeignKey(
                name: "FK_Item_Measures_MeasureID",
                table: "Item",
                column: "MeasureID",
                principalTable: "Measures",
                principalColumn: "MeasureID",
                onDelete: ReferentialAction.Restrict);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_Item_Measures_MeasureID",
                table: "Item");

            migrationBuilder.RenameColumn(
                name: "MeasureID",
                table: "Item",
                newName: "MeasuresMeasureID");

            migrationBuilder.RenameIndex(
                name: "IX_Item_MeasureID",
                table: "Item",
                newName: "IX_Item_MeasuresMeasureID");

            migrationBuilder.AddForeignKey(
                name: "FK_Item_Measures_MeasuresMeasureID",
                table: "Item",
                column: "MeasuresMeasureID",
                principalTable: "Measures",
                principalColumn: "MeasureID",
                onDelete: ReferentialAction.Restrict);
        }
    }
}