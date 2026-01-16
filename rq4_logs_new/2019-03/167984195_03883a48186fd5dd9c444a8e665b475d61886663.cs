using System;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;

namespace TRPR.Data.TRPRMigrations
{
    public partial class Audit : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "ResearchInstitutes",
                schema: "TRPR");

            migrationBuilder.DropColumn(
                name: "ResTitle",
                schema: "TRPR",
                table: "Researchers");

            migrationBuilder.DropColumn(
                name: "PaperType",
                schema: "TRPR",
                table: "PaperInfos");

            migrationBuilder.AddColumn<string>(
                name: "CreatedBy",
                schema: "TRPR",
                table: "ReviewAssigns",
                maxLength: 256,
                nullable: true);

            migrationBuilder.AddColumn<DateTime>(
                name: "CreatedOn",
                schema: "TRPR",
                table: "ReviewAssigns",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "UpdatedBy",
                schema: "TRPR",
                table: "ReviewAssigns",
                maxLength: 256,
                nullable: true);

            migrationBuilder.AddColumn<DateTime>(
                name: "UpdatedOn",
                schema: "TRPR",
                table: "ReviewAssigns",
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "InstituteID",
                schema: "TRPR",
                table: "Researchers",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<int>(
                name: "TitleID",
                schema: "TRPR",
                table: "Researchers",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<string>(
                name: "CreatedBy",
                schema: "TRPR",
                table: "PaperInfos",
                maxLength: 256,
                nullable: true);

            migrationBuilder.AddColumn<DateTime>(
                name: "CreatedOn",
                schema: "TRPR",
                table: "PaperInfos",
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "PaperTypeID",
                schema: "TRPR",
                table: "PaperInfos",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<string>(
                name: "UpdatedBy",
                schema: "TRPR",
                table: "PaperInfos",
                maxLength: 256,
                nullable: true);

            migrationBuilder.AddColumn<DateTime>(
                name: "UpdatedOn",
                schema: "TRPR",
                table: "PaperInfos",
                nullable: true);

            migrationBuilder.CreateTable(
                name: "PaperTypes",
                schema: "TRPR",
                columns: table => new
                {
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    Name = table.Column<string>(maxLength: 100, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PaperTypes", x => x.ID);
                });

            migrationBuilder.CreateTable(
                name: "Titles",
                schema: "TRPR",
                columns: table => new
                {
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    Name = table.Column<string>(maxLength: 10, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Titles", x => x.ID);
                });

            migrationBuilder.CreateIndex(
                name: "IX_Researchers_InstituteID",
                schema: "TRPR",
                table: "Researchers",
                column: "InstituteID");

            migrationBuilder.CreateIndex(
                name: "IX_Researchers_TitleID",
                schema: "TRPR",
                table: "Researchers",
                column: "TitleID");

            migrationBuilder.CreateIndex(
                name: "IX_PaperInfos_PaperTypeID",
                schema: "TRPR",
                table: "PaperInfos",
                column: "PaperTypeID");

            migrationBuilder.AddForeignKey(
                name: "FK_PaperInfos_PaperTypes_PaperTypeID",
                schema: "TRPR",
                table: "PaperInfos",
                column: "PaperTypeID",
                principalSchema: "TRPR",
                principalTable: "PaperTypes",
                principalColumn: "ID",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_Researchers_Institutes_InstituteID",
                schema: "TRPR",
                table: "Researchers",
                column: "InstituteID",
                principalSchema: "TRPR",
                principalTable: "Institutes",
                principalColumn: "ID",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_Researchers_Titles_TitleID",
                schema: "TRPR",
                table: "Researchers",
                column: "TitleID",
                principalSchema: "TRPR",
                principalTable: "Titles",
                principalColumn: "ID",
                onDelete: ReferentialAction.Cascade);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_PaperInfos_PaperTypes_PaperTypeID",
                schema: "TRPR",
                table: "PaperInfos");

            migrationBuilder.DropForeignKey(
                name: "FK_Researchers_Institutes_InstituteID",
                schema: "TRPR",
                table: "Researchers");

            migrationBuilder.DropForeignKey(
                name: "FK_Researchers_Titles_TitleID",
                schema: "TRPR",
                table: "Researchers");

            migrationBuilder.DropTable(
                name: "PaperTypes",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "Titles",
                schema: "TRPR");

            migrationBuilder.DropIndex(
                name: "IX_Researchers_InstituteID",
                schema: "TRPR",
                table: "Researchers");

            migrationBuilder.DropIndex(
                name: "IX_Researchers_TitleID",
                schema: "TRPR",
                table: "Researchers");

            migrationBuilder.DropIndex(
                name: "IX_PaperInfos_PaperTypeID",
                schema: "TRPR",
                table: "PaperInfos");

            migrationBuilder.DropColumn(
                name: "CreatedBy",
                schema: "TRPR",
                table: "ReviewAssigns");

            migrationBuilder.DropColumn(
                name: "CreatedOn",
                schema: "TRPR",
                table: "ReviewAssigns");

            migrationBuilder.DropColumn(
                name: "UpdatedBy",
                schema: "TRPR",
                table: "ReviewAssigns");

            migrationBuilder.DropColumn(
                name: "UpdatedOn",
                schema: "TRPR",
                table: "ReviewAssigns");

            migrationBuilder.DropColumn(
                name: "InstituteID",
                schema: "TRPR",
                table: "Researchers");

            migrationBuilder.DropColumn(
                name: "TitleID",
                schema: "TRPR",
                table: "Researchers");

            migrationBuilder.DropColumn(
                name: "CreatedBy",
                schema: "TRPR",
                table: "PaperInfos");

            migrationBuilder.DropColumn(
                name: "CreatedOn",
                schema: "TRPR",
                table: "PaperInfos");

            migrationBuilder.DropColumn(
                name: "PaperTypeID",
                schema: "TRPR",
                table: "PaperInfos");

            migrationBuilder.DropColumn(
                name: "UpdatedBy",
                schema: "TRPR",
                table: "PaperInfos");

            migrationBuilder.DropColumn(
                name: "UpdatedOn",
                schema: "TRPR",
                table: "PaperInfos");

            migrationBuilder.AddColumn<string>(
                name: "ResTitle",
                schema: "TRPR",
                table: "Researchers",
                maxLength: 10,
                nullable: false,
                defaultValue: "");

            migrationBuilder.AddColumn<string>(
                name: "PaperType",
                schema: "TRPR",
                table: "PaperInfos",
                maxLength: 30,
                nullable: false,
                defaultValue: "");

            migrationBuilder.CreateTable(
                name: "ResearchInstitutes",
                schema: "TRPR",
                columns: table => new
                {
                    ResearcherID = table.Column<int>(nullable: false),
                    InstituteID = table.Column<int>(nullable: false),
                    ResInstEndDate = table.Column<DateTime>(nullable: true),
                    ResInstStartDate = table.Column<DateTime>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ResearchInstitutes", x => new { x.ResearcherID, x.InstituteID });
                    table.ForeignKey(
                        name: "FK_ResearchInstitutes_Institutes_InstituteID",
                        column: x => x.InstituteID,
                        principalSchema: "TRPR",
                        principalTable: "Institutes",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_ResearchInstitutes_Researchers_ResearcherID",
                        column: x => x.ResearcherID,
                        principalSchema: "TRPR",
                        principalTable: "Researchers",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_ResearchInstitutes_InstituteID",
                schema: "TRPR",
                table: "ResearchInstitutes",
                column: "InstituteID");
        }
    }
}