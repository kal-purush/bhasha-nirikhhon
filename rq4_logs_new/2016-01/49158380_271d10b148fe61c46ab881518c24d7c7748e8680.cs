using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace com.reallifeministries.RockExtensions.Workflow.Migrations
{
    using Rock.Plugin;

    [MigrationNumber(1, "1.2.0")]
    public class _001_AddBirthdaySearchPageAndData : Migration
    {
        public override void Down()
        {
            RockMigrationHelper.DeleteAttribute("19AE5328-CF97-4191-AD15-DF8B0A9D67E2");
            RockMigrationHelper.DeleteAttribute("509F5E9B-B4DE-4AC3-A9C5-2B527093A198");
            RockMigrationHelper.DeleteBlock("3C08F81B-5FAF-43B7-A13B-6B8873A3B304");
            RockMigrationHelper.DeleteBlockType("61043136-18A6-4663-9379-3CAACE48607D");
            RockMigrationHelper.DeletePage("B759404A-7903-427F-921B-953A7E07A517"); //  Page: Birthday Search
        }

        public override void Up()
        {
            // Page: Birthday Search
            RockMigrationHelper.AddPage("5E036ADE-C2A4-4988-B393-DAC58230F02E", "D65F783D-87A9-4CC9-8110-E83466A0EADB", "Birthday Search", "", "B759404A-7903-427F-921B-953A7E07A517", ""); // Site:Rock RMS
            RockMigrationHelper.UpdateBlockType("Person Birthday Search", "Displays list of people that match a given search type and term.", "~/Plugins/com_reallifeministries/Search/PersonBirthdaySearch.ascx", "CRM", "61043136-18A6-4663-9379-3CAACE48607D");
            RockMigrationHelper.AddBlock("B759404A-7903-427F-921B-953A7E07A517", "", "61043136-18A6-4663-9379-3CAACE48607D", "Person Birthday Search", "Main", "", "", 0, "3C08F81B-5FAF-43B7-A13B-6B8873A3B304");
            RockMigrationHelper.AddBlockTypeAttribute("61043136-18A6-4663-9379-3CAACE48607D", "1EDAFDED-DFE6-4334-B019-6EECBA89E05A", "Show Performance", "ShowPerformance", "", "Displays how long the search took.", 0, @"False", "509F5E9B-B4DE-4AC3-A9C5-2B527093A198");
            RockMigrationHelper.AddBlockTypeAttribute("61043136-18A6-4663-9379-3CAACE48607D", "BD53F9C9-EBA9-4D3F-82EA-DE5DD34A8108", "Person Detail Page", "PersonDetailPage", "", "", 0, @"", "19AE5328-CF97-4191-AD15-DF8B0A9D67E2");
            RockMigrationHelper.AddBlockAttributeValue("3C08F81B-5FAF-43B7-A13B-6B8873A3B304", "509F5E9B-B4DE-4AC3-A9C5-2B527093A198", @"True"); // Show Performance
            RockMigrationHelper.AddBlockAttributeValue("3C08F81B-5FAF-43B7-A13B-6B8873A3B304", "19AE5328-CF97-4191-AD15-DF8B0A9D67E2", @"08dbd8a5-2c35-4146-b4a8-0f7652348b25"); // Person Detail Page

        }
    }
}