using BLL.Interface.Entities;
using System.Data.Entity;

namespace DAL.Repositories
{
    class AccountContext : DbContext
    {
        static AccountContext()
        {
            Database.SetInitializer<AccountContext>(new DropCreateDatabaseAlways<AccountContext>());
        }

        public AccountContext() : base("DefaultConnection")
        { }

        public DbSet<Account> Accounts { get; set; }
    }
}