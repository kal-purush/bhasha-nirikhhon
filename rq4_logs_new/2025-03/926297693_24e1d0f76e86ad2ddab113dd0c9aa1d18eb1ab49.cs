using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace SPSS.Repository.Migrations
{
    /// <inheritdoc />
    public partial class DBlocal : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_Result_SkinTypes_SkinTypeId",
                table: "Result");

            migrationBuilder.DropPrimaryKey(
                name: "PK_Result",
                table: "Result");

            migrationBuilder.RenameTable(
                name: "Result",
                newName: "Results");

            migrationBuilder.RenameIndex(
                name: "IX_Result_SkinTypeId",
                table: "Results",
                newName: "IX_Results_SkinTypeId");

            migrationBuilder.AddPrimaryKey(
                name: "PK_Results",
                table: "Results",
                column: "Id");

            migrationBuilder.AddForeignKey(
                name: "FK_Results_SkinTypes_SkinTypeId",
                table: "Results",
                column: "SkinTypeId",
                principalTable: "SkinTypes",
                principalColumn: "Id",
                onDelete: ReferentialAction.Cascade);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_Results_SkinTypes_SkinTypeId",
                table: "Results");

            migrationBuilder.DropPrimaryKey(
                name: "PK_Results",
                table: "Results");

            migrationBuilder.RenameTable(
                name: "Results",
                newName: "Result");

            migrationBuilder.RenameIndex(
                name: "IX_Results_SkinTypeId",
                table: "Result",
                newName: "IX_Result_SkinTypeId");

            migrationBuilder.AddPrimaryKey(
                name: "PK_Result",
                table: "Result",
                column: "Id");

            migrationBuilder.AddForeignKey(
                name: "FK_Result_SkinTypes_SkinTypeId",
                table: "Result",
                column: "SkinTypeId",
                principalTable: "SkinTypes",
                principalColumn: "Id",
                onDelete: ReferentialAction.Cascade);
        }
    }
}