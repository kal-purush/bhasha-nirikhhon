using LT.DigitalOffice.UserService.Models.Db;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace LT.DigitalOffice.UserService.Data.Provider.MsSql.Ef.Migrations
{
    [DbContext(typeof(UserServiceDbContext))]
    [Migration("20210414152800_AddEducationTypeToCertificatesTabel")]
    class AddEducationTypeToCertificatesTabel : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                table: DbUserCertificate.TableName,
                name: nameof(DbUserCertificate.EducationType),
                nullable: false);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                table: DbUserCertificate.TableName,
                name: nameof(DbUserCertificate.EducationType));
        }
    }
}