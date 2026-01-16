using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace LT.DigitalOffice.UserService.Data.Provider.MsSql.Ef.Migrations
{
  [DbContext(typeof(UserServiceDbContext))]
  [Migration("20211216161500_DropSkillsAndUserSkillsTables")]
  public class DropSkillsAndUserSkillsTables : Migration
  {
    protected override void Up(MigrationBuilder builder)
    {
      builder.DropTable(
        name: "UserSkills");

      builder.DropTable(
        name: "Skills");
    }
  }
}