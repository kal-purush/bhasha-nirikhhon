using System;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;

namespace CRM.Data.Migrations
{
    public partial class MergeGroupAndTimeTable : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_Students_Groups_GroupId",
                table: "Students");

            migrationBuilder.DropTable(
                name: "Groups");

            migrationBuilder.AddColumn<int>(
                name: "BranchId",
                table: "TimeTables",
                nullable: true);

            migrationBuilder.AddColumn<DateTime>(
                name: "DateCreated",
                table: "TimeTables",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "UserId",
                table: "TimeTables",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "Discriminator",
                table: "TimeTables",
                nullable: false,
                defaultValue: "");

            migrationBuilder.CreateIndex(
                name: "IX_TimeTables_BranchId",
                table: "TimeTables",
                column: "BranchId");

            migrationBuilder.CreateIndex(
                name: "IX_TimeTables_UserId",
                table: "TimeTables",
                column: "UserId");

            migrationBuilder.AddForeignKey(
                name: "FK_Students_TimeTables_GroupId",
                table: "Students",
                column: "GroupId",
                principalTable: "TimeTables",
                principalColumn: "Id",
                onDelete: ReferentialAction.Restrict);

            migrationBuilder.AddForeignKey(
                name: "FK_TimeTables_Branches_BranchId",
                table: "TimeTables",
                column: "BranchId",
                principalTable: "Branches",
                principalColumn: "Id",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_TimeTables_AspNetUsers_UserId",
                table: "TimeTables",
                column: "UserId",
                principalTable: "AspNetUsers",
                principalColumn: "Id",
                onDelete: ReferentialAction.Restrict);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_Students_TimeTables_GroupId",
                table: "Students");

            migrationBuilder.DropForeignKey(
                name: "FK_TimeTables_Branches_BranchId",
                table: "TimeTables");

            migrationBuilder.DropForeignKey(
                name: "FK_TimeTables_AspNetUsers_UserId",
                table: "TimeTables");

            migrationBuilder.DropIndex(
                name: "IX_TimeTables_BranchId",
                table: "TimeTables");

            migrationBuilder.DropIndex(
                name: "IX_TimeTables_UserId",
                table: "TimeTables");

            migrationBuilder.DropColumn(
                name: "BranchId",
                table: "TimeTables");

            migrationBuilder.DropColumn(
                name: "DateCreated",
                table: "TimeTables");

            migrationBuilder.DropColumn(
                name: "UserId",
                table: "TimeTables");

            migrationBuilder.DropColumn(
                name: "Discriminator",
                table: "TimeTables");

            migrationBuilder.CreateTable(
                name: "Groups",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    BranchId = table.Column<int>(nullable: false),
                    DateCreated = table.Column<DateTime>(nullable: false),
                    TimeTableId = table.Column<int>(nullable: false),
                    UserId = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Groups", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Groups_Branches_BranchId",
                        column: x => x.BranchId,
                        principalTable: "Branches",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_Groups_TimeTables_TimeTableId",
                        column: x => x.TimeTableId,
                        principalTable: "TimeTables",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_Groups_AspNetUsers_UserId",
                        column: x => x.UserId,
                        principalTable: "AspNetUsers",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateIndex(
                name: "IX_Groups_BranchId",
                table: "Groups",
                column: "BranchId");

            migrationBuilder.CreateIndex(
                name: "IX_Groups_TimeTableId",
                table: "Groups",
                column: "TimeTableId");

            migrationBuilder.CreateIndex(
                name: "IX_Groups_UserId",
                table: "Groups",
                column: "UserId");

            migrationBuilder.AddForeignKey(
                name: "FK_Students_Groups_GroupId",
                table: "Students",
                column: "GroupId",
                principalTable: "Groups",
                principalColumn: "Id",
                onDelete: ReferentialAction.Restrict);
        }
    }
}