using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace com.reallifeministries.RockExtensions.Migrations
{
    using Rock.Plugin;

    [MigrationNumber(2, "1.2.0")]
    public class _002_AddBirthdaySearchPageAndData : Migration
    {
        public override void Down()
        {
        }

        public override void Up()
        {
            // Page: Birthday Search            
            RockMigrationHelper.AddPageRoute("B759404A-7903-427F-921B-953A7E07A517", "Person/Search/Birthday");
        }
    }
}