using Microsoft.EntityFrameworkCore.Migrations;

namespace Data.Migrations
{
    public partial class IdentityRoles : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.InsertData(
                table: "AspNetRoles",
                columns: new[] { "Id", "ConcurrencyStamp", "Name", "NormalizedName" },
                values: new object[] { "a4035352-12e4-4b10-a6ce-bd5cf8fdeadc", "0ad977da-e489-4816-9d65-d6cead6e6a10", "GeneralUser", "GENERALUSER" });

            migrationBuilder.InsertData(
                table: "AspNetRoles",
                columns: new[] { "Id", "ConcurrencyStamp", "Name", "NormalizedName" },
                values: new object[] { "223d4071-73b5-42d3-b76b-9302887e3b79", "0120078d-cd47-4b77-a494-2dd0bbc9a7bc", "RobinAdmin", "ROBINADMIN" });

            migrationBuilder.InsertData(
                table: "AspNetRoles",
                columns: new[] { "Id", "ConcurrencyStamp", "Name", "NormalizedName" },
                values: new object[] { "61f14167-ef95-4389-987e-270b513e5b23", "fca5905e-abcb-43c5-9ff1-447f6f2e4883", "SuperAdmin", "SUPERADMIN" });
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DeleteData(
                table: "AspNetRoles",
                keyColumns: new[] { "Id", "ConcurrencyStamp" },
                keyValues: new object[] { "223d4071-73b5-42d3-b76b-9302887e3b79", "0120078d-cd47-4b77-a494-2dd0bbc9a7bc" });

            migrationBuilder.DeleteData(
                table: "AspNetRoles",
                keyColumns: new[] { "Id", "ConcurrencyStamp" },
                keyValues: new object[] { "61f14167-ef95-4389-987e-270b513e5b23", "fca5905e-abcb-43c5-9ff1-447f6f2e4883" });

            migrationBuilder.DeleteData(
                table: "AspNetRoles",
                keyColumns: new[] { "Id", "ConcurrencyStamp" },
                keyValues: new object[] { "a4035352-12e4-4b10-a6ce-bd5cf8fdeadc", "0ad977da-e489-4816-9d65-d6cead6e6a10" });
        }
    }
}