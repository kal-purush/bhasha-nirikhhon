using System.Data.Entity.Migrations;
using hGem.Data.Migrations.DataSeed;

namespace hGem.Data.Migrations
{
    internal sealed class Configuration : DbMigrationsConfiguration<MigrationsAndTestingContext>
    {
        public Configuration()
        {
            AutomaticMigrationsEnabled = false;
        }

        protected override void Seed(MigrationsAndTestingContext context)
        {
            DataFactoryResolver.Seed(context);
        }
    }
}