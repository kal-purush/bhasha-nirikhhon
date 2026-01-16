using Microsoft.EntityFrameworkCore.Migrations;

namespace MVCswitchback.Migrations
{
    public partial class AddsSeedsForID1 : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.InsertData(
                table: "UserReviews",
                columns: new[] { "ID", "TrailID", "UserComment", "UserID", "UserInfoID" },
                values: new object[,]
                {
                    { 6, 1, "My trailmates were all slow, but the trail was great.", 1, null },
                    { 7, 1, "I don't like physical activity...", 2, null },
                    { 8, 1, "Was much fun, has difficult. much peril. 12/10 would recommend for bamboozle.", 3, null },
                    { 9, 1, "It was ok.", 4, null },
                    { 10, 1, "The trail was fantastic and the views were amazing.", 5, null }
                });
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DeleteData(
                table: "UserReviews",
                keyColumn: "ID",
                keyValue: 6);

            migrationBuilder.DeleteData(
                table: "UserReviews",
                keyColumn: "ID",
                keyValue: 7);

            migrationBuilder.DeleteData(
                table: "UserReviews",
                keyColumn: "ID",
                keyValue: 8);

            migrationBuilder.DeleteData(
                table: "UserReviews",
                keyColumn: "ID",
                keyValue: 9);

            migrationBuilder.DeleteData(
                table: "UserReviews",
                keyColumn: "ID",
                keyValue: 10);
        }
    }
}