using Microsoft.EntityFrameworkCore.Migrations;

namespace MAJServices.Migrations
{
    public partial class UsersSecurityStamp : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DeleteData(
                table: "AspNetRoles",
                keyColumns: new[] { "Id", "ConcurrencyStamp" },
                keyValues: new object[] { "4722f133-1089-4488-96f0-f672c9b5b4bc", "66b9dc93-ff24-41ce-8dea-67cf80a6b80a" });

            migrationBuilder.DeleteData(
                table: "AspNetRoles",
                keyColumns: new[] { "Id", "ConcurrencyStamp" },
                keyValues: new object[] { "8a163f6c-cc83-437a-b62d-9dc629c78882", "b1cdde56-aa49-481b-8f1f-4fb21f002a67" });

            migrationBuilder.DeleteData(
                table: "AspNetRoles",
                keyColumns: new[] { "Id", "ConcurrencyStamp" },
                keyValues: new object[] { "93779d28-614d-401e-ac06-006e384e077b", "a40841ad-c3e6-4941-a2a3-76ea1f6d9940" });

            migrationBuilder.DeleteData(
                table: "AspNetRoles",
                keyColumns: new[] { "Id", "ConcurrencyStamp" },
                keyValues: new object[] { "fd57ff74-8930-4259-97af-f118d66e84fe", "9d89b675-18e5-4717-9ad5-61df9d3de11f" });

            migrationBuilder.InsertData(
                table: "AspNetRoles",
                columns: new[] { "Id", "ConcurrencyStamp", "Name", "NormalizedName" },
                values: new object[,]
                {
                    { "01623f6e-839b-488f-bcf6-b0823b60a7e4", "1287dec3-cdea-49d6-8b7b-4df56af28be1", "SuperAdmin", "SUPERADMIN" },
                    { "7ade0ced-7739-4be1-9019-4d310bef3beb", "95447237-21c5-43f7-b065-7bbe6fa33ca6", "Admin", "ADMIN" },
                    { "76d902c3-5332-4698-81d1-3d83c07f4a25", "2b74cc46-c85d-49b3-813f-57f23b87ed3d", "Subadmin", "SUBADMIN" },
                    { "f94a86cd-c4c2-44e5-b6c7-f407e01d4105", "f0d3a2eb-55ed-478f-b060-7559aa6fa71a", "General", "GENERAL" }
                });

            migrationBuilder.UpdateData(
                table: "AspNetUsers",
                keyColumn: "Id",
                keyValue: "2014193056",
                columns: new[] { "ConcurrencyStamp", "SecurityStamp" },
                values: new object[] { "1cc83d86-1f9a-4dd4-af90-1242e14e4698", "1406e2e3-5f80-48f9-a5f8-7138476b5af1" });

            migrationBuilder.UpdateData(
                table: "AspNetUsers",
                keyColumn: "Id",
                keyValue: "2014378223",
                columns: new[] { "ConcurrencyStamp", "SecurityStamp" },
                values: new object[] { "07076940-5ad9-4a39-890c-49f80b638c96", "6ae71fe3-82de-46a0-a734-053a5cf59d49" });

            migrationBuilder.UpdateData(
                table: "AspNetUsers",
                keyColumn: "Id",
                keyValue: "2014630132",
                columns: new[] { "ConcurrencyStamp", "SecurityStamp" },
                values: new object[] { "f5e3ae9c-4d40-49a0-af2c-c111aa453e3f", "8ac84725-b48d-4fc0-b43e-5519d8101f84" });

            migrationBuilder.UpdateData(
                table: "AspNetUsers",
                keyColumn: "Id",
                keyValue: "2014631903",
                columns: new[] { "ConcurrencyStamp", "SecurityStamp" },
                values: new object[] { "e5a23e15-9532-4ccf-9328-301f33f0a95b", "c8686797-31ed-40cf-9a6c-1845ae332e45" });
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DeleteData(
                table: "AspNetRoles",
                keyColumns: new[] { "Id", "ConcurrencyStamp" },
                keyValues: new object[] { "01623f6e-839b-488f-bcf6-b0823b60a7e4", "1287dec3-cdea-49d6-8b7b-4df56af28be1" });

            migrationBuilder.DeleteData(
                table: "AspNetRoles",
                keyColumns: new[] { "Id", "ConcurrencyStamp" },
                keyValues: new object[] { "76d902c3-5332-4698-81d1-3d83c07f4a25", "2b74cc46-c85d-49b3-813f-57f23b87ed3d" });

            migrationBuilder.DeleteData(
                table: "AspNetRoles",
                keyColumns: new[] { "Id", "ConcurrencyStamp" },
                keyValues: new object[] { "7ade0ced-7739-4be1-9019-4d310bef3beb", "95447237-21c5-43f7-b065-7bbe6fa33ca6" });

            migrationBuilder.DeleteData(
                table: "AspNetRoles",
                keyColumns: new[] { "Id", "ConcurrencyStamp" },
                keyValues: new object[] { "f94a86cd-c4c2-44e5-b6c7-f407e01d4105", "f0d3a2eb-55ed-478f-b060-7559aa6fa71a" });

            migrationBuilder.InsertData(
                table: "AspNetRoles",
                columns: new[] { "Id", "ConcurrencyStamp", "Name", "NormalizedName" },
                values: new object[,]
                {
                    { "fd57ff74-8930-4259-97af-f118d66e84fe", "9d89b675-18e5-4717-9ad5-61df9d3de11f", "SuperAdmin", "SUPERADMIN" },
                    { "4722f133-1089-4488-96f0-f672c9b5b4bc", "66b9dc93-ff24-41ce-8dea-67cf80a6b80a", "Admin", "ADMIN" },
                    { "8a163f6c-cc83-437a-b62d-9dc629c78882", "b1cdde56-aa49-481b-8f1f-4fb21f002a67", "Subadmin", "SUBADMIN" },
                    { "93779d28-614d-401e-ac06-006e384e077b", "a40841ad-c3e6-4941-a2a3-76ea1f6d9940", "General", "GENERAL" }
                });

            migrationBuilder.UpdateData(
                table: "AspNetUsers",
                keyColumn: "Id",
                keyValue: "2014193056",
                columns: new[] { "ConcurrencyStamp", "SecurityStamp" },
                values: new object[] { "6e66d7b8-53d6-47e8-bfaf-e03be2466823", null });

            migrationBuilder.UpdateData(
                table: "AspNetUsers",
                keyColumn: "Id",
                keyValue: "2014378223",
                columns: new[] { "ConcurrencyStamp", "SecurityStamp" },
                values: new object[] { "f136891d-6f6c-4e77-9b7e-3e06304c7a9d", null });

            migrationBuilder.UpdateData(
                table: "AspNetUsers",
                keyColumn: "Id",
                keyValue: "2014630132",
                columns: new[] { "ConcurrencyStamp", "SecurityStamp" },
                values: new object[] { "1b981882-5961-4dbb-bd80-1687b207d803", null });

            migrationBuilder.UpdateData(
                table: "AspNetUsers",
                keyColumn: "Id",
                keyValue: "2014631903",
                columns: new[] { "ConcurrencyStamp", "SecurityStamp" },
                values: new object[] { "c3cae711-9fa7-4ce0-845c-b9f3c5316cac", null });
        }
    }
}