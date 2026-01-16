using System;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;

namespace TRPR.Data.TRPRMigrations
{
    public partial class Initial : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.EnsureSchema(
                name: "TRPR");

            migrationBuilder.CreateTable(
                name: "Expertises",
                schema: "TRPR",
                columns: table => new
                {
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    ExpName = table.Column<string>(maxLength: 30, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Expertises", x => x.ID);
                });

            migrationBuilder.CreateTable(
                name: "FileTypes",
                schema: "TRPR",
                columns: table => new
                {
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    TypeName = table.Column<string>(maxLength: 50, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_FileTypes", x => x.ID);
                });

            migrationBuilder.CreateTable(
                name: "Institutes",
                schema: "TRPR",
                columns: table => new
                {
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    InstName = table.Column<string>(maxLength: 100, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Institutes", x => x.ID);
                });

            migrationBuilder.CreateTable(
                name: "Keywords",
                schema: "TRPR",
                columns: table => new
                {
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    KeyWord = table.Column<string>(maxLength: 100, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Keywords", x => x.ID);
                });

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
                name: "Recommends",
                schema: "TRPR",
                columns: table => new
                {
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    RecTitle = table.Column<string>(maxLength: 20, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Recommends", x => x.ID);
                });

            migrationBuilder.CreateTable(
                name: "ReviewAgains",
                schema: "TRPR",
                columns: table => new
                {
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    ReSponse = table.Column<bool>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ReviewAgains", x => x.ID);
                });

            migrationBuilder.CreateTable(
                name: "Roles",
                schema: "TRPR",
                columns: table => new
                {
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    RoleTitle = table.Column<string>(maxLength: 20, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Roles", x => x.ID);
                });

            migrationBuilder.CreateTable(
                name: "Statuses",
                schema: "TRPR",
                columns: table => new
                {
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    StatName = table.Column<string>(maxLength: 20, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Statuses", x => x.ID);
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

            migrationBuilder.CreateTable(
                name: "PaperInfos",
                schema: "TRPR",
                columns: table => new
                {
                    CreatedBy = table.Column<string>(maxLength: 256, nullable: true),
                    CreatedOn = table.Column<DateTime>(nullable: true),
                    UpdatedBy = table.Column<string>(maxLength: 256, nullable: true),
                    UpdatedOn = table.Column<DateTime>(nullable: true),
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    PaperTitle = table.Column<string>(maxLength: 200, nullable: false),
                    PaperAbstract = table.Column<string>(maxLength: 500, nullable: false),
                    PaperTypeID = table.Column<int>(nullable: false),
                    PaperLength = table.Column<int>(nullable: false),
                    StatusID = table.Column<int>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PaperInfos", x => x.ID);
                    table.ForeignKey(
                        name: "FK_PaperInfos_PaperTypes_PaperTypeID",
                        column: x => x.PaperTypeID,
                        principalSchema: "TRPR",
                        principalTable: "PaperTypes",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_PaperInfos_Statuses_StatusID",
                        column: x => x.StatusID,
                        principalSchema: "TRPR",
                        principalTable: "Statuses",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Researchers",
                schema: "TRPR",
                columns: table => new
                {
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    TitleID = table.Column<int>(nullable: false),
                    ResFirst = table.Column<string>(maxLength: 50, nullable: false),
                    ResMiddle = table.Column<string>(maxLength: 50, nullable: true),
                    ResLast = table.Column<string>(maxLength: 50, nullable: false),
                    ResEmail = table.Column<string>(maxLength: 320, nullable: false),
                    ResBio = table.Column<string>(maxLength: 500, nullable: true),
                    InstituteID = table.Column<int>(nullable: false),
                    Active = table.Column<bool>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Researchers", x => x.ID);
                    table.ForeignKey(
                        name: "FK_Researchers_Institutes_InstituteID",
                        column: x => x.InstituteID,
                        principalSchema: "TRPR",
                        principalTable: "Institutes",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_Researchers_Titles_TitleID",
                        column: x => x.TitleID,
                        principalSchema: "TRPR",
                        principalTable: "Titles",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "PaperKeywords",
                schema: "TRPR",
                columns: table => new
                {
                    PaperInfoID = table.Column<int>(nullable: false),
                    KeywordID = table.Column<int>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PaperKeywords", x => new { x.PaperInfoID, x.KeywordID });
                    table.ForeignKey(
                        name: "FK_PaperKeywords_Keywords_KeywordID",
                        column: x => x.KeywordID,
                        principalSchema: "TRPR",
                        principalTable: "Keywords",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_PaperKeywords_PaperInfos_PaperInfoID",
                        column: x => x.PaperInfoID,
                        principalSchema: "TRPR",
                        principalTable: "PaperInfos",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "AuthoredPapers",
                schema: "TRPR",
                columns: table => new
                {
                    ResearcherID = table.Column<int>(nullable: false),
                    PaperInfoID = table.Column<int>(nullable: false),
                    AuthPapLevel = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AuthoredPapers", x => new { x.ResearcherID, x.PaperInfoID });
                    table.ForeignKey(
                        name: "FK_AuthoredPapers_PaperInfos_PaperInfoID",
                        column: x => x.PaperInfoID,
                        principalSchema: "TRPR",
                        principalTable: "PaperInfos",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_AuthoredPapers_Researchers_ResearcherID",
                        column: x => x.ResearcherID,
                        principalSchema: "TRPR",
                        principalTable: "Researchers",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "ResearchExpertises",
                schema: "TRPR",
                columns: table => new
                {
                    ExpertiseID = table.Column<int>(nullable: false),
                    ResearcherID = table.Column<int>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ResearchExpertises", x => new { x.ResearcherID, x.ExpertiseID });
                    table.ForeignKey(
                        name: "FK_ResearchExpertises_Expertises_ExpertiseID",
                        column: x => x.ExpertiseID,
                        principalSchema: "TRPR",
                        principalTable: "Expertises",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_ResearchExpertises_Researchers_ResearcherID",
                        column: x => x.ResearcherID,
                        principalSchema: "TRPR",
                        principalTable: "Researchers",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "ReviewAssigns",
                schema: "TRPR",
                columns: table => new
                {
                    CreatedBy = table.Column<string>(maxLength: 256, nullable: true),
                    CreatedOn = table.Column<DateTime>(nullable: true),
                    UpdatedBy = table.Column<string>(maxLength: 256, nullable: true),
                    UpdatedOn = table.Column<DateTime>(nullable: true),
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    PaperInfoID = table.Column<int>(nullable: false),
                    ResearcherID = table.Column<int>(nullable: false),
                    RoleID = table.Column<int>(nullable: false),
                    RevContentReview = table.Column<string>(nullable: true),
                    RevKeywordReview = table.Column<string>(nullable: true),
                    RevLengthReview = table.Column<string>(nullable: true),
                    RevFormatReview = table.Column<string>(nullable: true),
                    RevCitationReview = table.Column<string>(nullable: true),
                    RecommendID = table.Column<int>(nullable: false),
                    ReviewAgainID = table.Column<int>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ReviewAssigns", x => x.ID);
                    table.ForeignKey(
                        name: "FK_ReviewAssigns_PaperInfos_PaperInfoID",
                        column: x => x.PaperInfoID,
                        principalSchema: "TRPR",
                        principalTable: "PaperInfos",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_ReviewAssigns_Recommends_RecommendID",
                        column: x => x.RecommendID,
                        principalSchema: "TRPR",
                        principalTable: "Recommends",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_ReviewAssigns_Researchers_ResearcherID",
                        column: x => x.ResearcherID,
                        principalSchema: "TRPR",
                        principalTable: "Researchers",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_ReviewAssigns_ReviewAgains_ReviewAgainID",
                        column: x => x.ReviewAgainID,
                        principalSchema: "TRPR",
                        principalTable: "ReviewAgains",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_ReviewAssigns_Roles_RoleID",
                        column: x => x.RoleID,
                        principalSchema: "TRPR",
                        principalTable: "Roles",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Comments",
                schema: "TRPR",
                columns: table => new
                {
                    CreatedBy = table.Column<string>(maxLength: 256, nullable: true),
                    CreatedOn = table.Column<DateTime>(nullable: true),
                    UpdatedBy = table.Column<string>(maxLength: 256, nullable: true),
                    UpdatedOn = table.Column<DateTime>(nullable: true),
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    RevID = table.Column<int>(nullable: false),
                    ReviewAssignID = table.Column<int>(nullable: true),
                    ComAccess = table.Column<string>(maxLength: 50, nullable: false),
                    Comtext = table.Column<string>(maxLength: 500, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Comments", x => x.ID);
                    table.ForeignKey(
                        name: "FK_Comments_ReviewAssigns_ReviewAssignID",
                        column: x => x.ReviewAssignID,
                        principalSchema: "TRPR",
                        principalTable: "ReviewAssigns",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "Files",
                schema: "TRPR",
                columns: table => new
                {
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    FileName = table.Column<string>(nullable: false),
                    TypeID = table.Column<int>(nullable: false),
                    FileTypeID = table.Column<int>(nullable: true),
                    PaperInfoID = table.Column<int>(nullable: true),
                    ReveiwAssignID = table.Column<int>(nullable: true),
                    ReviewAssignID = table.Column<int>(nullable: true),
                    FileContent = table.Column<byte[]>(nullable: true),
                    FileMimeType = table.Column<string>(maxLength: 256, nullable: true),
                    CreatedBy = table.Column<string>(maxLength: 256, nullable: true),
                    CreatedOn = table.Column<DateTime>(nullable: true),
                    UpdatedBy = table.Column<string>(maxLength: 256, nullable: true),
                    UpdatedOn = table.Column<DateTime>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Files", x => x.ID);
                    table.ForeignKey(
                        name: "FK_Files_FileTypes_FileTypeID",
                        column: x => x.FileTypeID,
                        principalSchema: "TRPR",
                        principalTable: "FileTypes",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_Files_PaperInfos_PaperInfoID",
                        column: x => x.PaperInfoID,
                        principalSchema: "TRPR",
                        principalTable: "PaperInfos",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_Files_ReviewAssigns_ReviewAssignID",
                        column: x => x.ReviewAssignID,
                        principalSchema: "TRPR",
                        principalTable: "ReviewAssigns",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateIndex(
                name: "IX_AuthoredPapers_PaperInfoID",
                schema: "TRPR",
                table: "AuthoredPapers",
                column: "PaperInfoID");

            migrationBuilder.CreateIndex(
                name: "IX_Comments_ReviewAssignID",
                schema: "TRPR",
                table: "Comments",
                column: "ReviewAssignID");

            migrationBuilder.CreateIndex(
                name: "IX_Files_FileTypeID",
                schema: "TRPR",
                table: "Files",
                column: "FileTypeID");

            migrationBuilder.CreateIndex(
                name: "IX_Files_PaperInfoID",
                schema: "TRPR",
                table: "Files",
                column: "PaperInfoID");

            migrationBuilder.CreateIndex(
                name: "IX_Files_ReviewAssignID",
                schema: "TRPR",
                table: "Files",
                column: "ReviewAssignID");

            migrationBuilder.CreateIndex(
                name: "IX_PaperInfos_PaperTypeID",
                schema: "TRPR",
                table: "PaperInfos",
                column: "PaperTypeID");

            migrationBuilder.CreateIndex(
                name: "IX_PaperInfos_StatusID",
                schema: "TRPR",
                table: "PaperInfos",
                column: "StatusID");

            migrationBuilder.CreateIndex(
                name: "IX_PaperKeywords_KeywordID",
                schema: "TRPR",
                table: "PaperKeywords",
                column: "KeywordID");

            migrationBuilder.CreateIndex(
                name: "IX_Researchers_InstituteID",
                schema: "TRPR",
                table: "Researchers",
                column: "InstituteID");

            migrationBuilder.CreateIndex(
                name: "IX_Researchers_ResEmail",
                schema: "TRPR",
                table: "Researchers",
                column: "ResEmail",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Researchers_TitleID",
                schema: "TRPR",
                table: "Researchers",
                column: "TitleID");

            migrationBuilder.CreateIndex(
                name: "IX_ResearchExpertises_ExpertiseID",
                schema: "TRPR",
                table: "ResearchExpertises",
                column: "ExpertiseID");

            migrationBuilder.CreateIndex(
                name: "IX_ReviewAssigns_PaperInfoID",
                schema: "TRPR",
                table: "ReviewAssigns",
                column: "PaperInfoID");

            migrationBuilder.CreateIndex(
                name: "IX_ReviewAssigns_RecommendID",
                schema: "TRPR",
                table: "ReviewAssigns",
                column: "RecommendID");

            migrationBuilder.CreateIndex(
                name: "IX_ReviewAssigns_ResearcherID",
                schema: "TRPR",
                table: "ReviewAssigns",
                column: "ResearcherID");

            migrationBuilder.CreateIndex(
                name: "IX_ReviewAssigns_ReviewAgainID",
                schema: "TRPR",
                table: "ReviewAssigns",
                column: "ReviewAgainID");

            migrationBuilder.CreateIndex(
                name: "IX_ReviewAssigns_RoleID",
                schema: "TRPR",
                table: "ReviewAssigns",
                column: "RoleID");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "AuthoredPapers",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "Comments",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "Files",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "PaperKeywords",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "ResearchExpertises",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "FileTypes",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "ReviewAssigns",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "Keywords",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "Expertises",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "PaperInfos",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "Recommends",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "Researchers",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "ReviewAgains",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "Roles",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "PaperTypes",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "Statuses",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "Institutes",
                schema: "TRPR");

            migrationBuilder.DropTable(
                name: "Titles",
                schema: "TRPR");
        }
    }
}