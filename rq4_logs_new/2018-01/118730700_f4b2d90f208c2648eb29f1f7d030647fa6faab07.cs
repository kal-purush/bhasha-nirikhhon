using AndersonNotificationEntity;
using System.Data.Entity;

namespace AndersonNotificationContext
{
    public class Context : DbContext
    {
        public Context() : base("Notification")
        {

            if (Database.Exists())
            {
                Database.SetInitializer(new MigrateDatabaseToLatestVersion<Context, Migrations.Configuration>());
            }
            else
            {
                Database.SetInitializer(new DBInitializer());
            }
        }

        public DbSet<ENotification> Notification { get; set; }
    }

}