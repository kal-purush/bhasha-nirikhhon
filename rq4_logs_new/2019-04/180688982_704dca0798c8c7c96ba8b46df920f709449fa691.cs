using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;

namespace MVCswitchback.Migrations
{
    public partial class _1 : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "UserInfo",
                columns: table => new
                {
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    UserName = table.Column<string>(nullable: true),
                    FirstName = table.Column<string>(nullable: true),
                    LastName = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_UserInfo", x => x.ID);
                });

            migrationBuilder.CreateTable(
                name: "UserReviews",
                columns: table => new
                {
                    ID = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    UserID = table.Column<int>(nullable: false),
                    TrailID = table.Column<int>(nullable: false),
                    UserComment = table.Column<string>(nullable: true),
                    UserInfoID = table.Column<int>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_UserReviews", x => x.ID);
                    table.ForeignKey(
                        name: "FK_UserReviews_UserInfo_UserInfoID",
                        column: x => x.UserInfoID,
                        principalTable: "UserInfo",
                        principalColumn: "ID",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.InsertData(
                table: "UserInfo",
                columns: new[] { "ID", "FirstName", "LastName", "UserName" },
                values: new object[,]
                {
                    { 1, "Christopher", "Morton", "Secret Squirrel" },
                    { 2, "Andy", "Roska", "Roketsu" },
                    { 3, "Ian", "Gifford", "Hype Man" },
                    { 4, "Tanner", "Percival", "TannMann" },
                    { 5, "Mike", "Kelly", "The Wizard" }
                });

            migrationBuilder.InsertData(
                table: "UserReviews",
                columns: new[] { "ID", "TrailID", "UserComment", "UserID", "UserInfoID" },
                values: new object[,]
                {
                    { 1, 7013499, "My trailmates were all slow, but the trail was great.", 1, null },
                    { 2, 7013499, "I don't like physical activity...", 2, null },
                    { 3, 7013499, "Was much fun, has difficult. much peril. 12/10 would recommend for bamboozle.", 3, null },
                    { 4, 7013499, "It was ok.", 4, null },
                    { 5, 7013499, "The trail was fantastic and the views were amazing.", 5, null },
                    { 6, 1, "My trailmates were all slow, but the trail was great.", 1, null },
                    { 7, 1, "I don't like physical activity...", 2, null },
                    { 8, 1, "Was much fun, has difficult. much peril. 12/10 would recommend for bamboozle.", 3, null },
                    { 9, 1, "It was ok.", 4, null },
                    { 10, 1, "The trail was fantastic and the views were amazing.", 5, null }
                });

            migrationBuilder.CreateIndex(
                name: "IX_UserReviews_UserInfoID",
                table: "UserReviews",
                column: "UserInfoID");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "UserReviews");

            migrationBuilder.DropTable(
                name: "UserInfo");
        }
    }
}