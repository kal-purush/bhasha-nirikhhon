using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Entity;

using Discriminator.Model;

namespace Discriminator.DAL{
    public class Myconfiguration : DbConfiguration
    {
        public Myconfiguration()
        {
            SetDefaultConnectionFactory(new System.Data.Entity.Infrastructure.LocalDbConnectionFactory("mssqllocaldb"));
//            Database.SetInitializer(new DropCreateDatabaseIfModelChanges<EfDbContext>());
        }
    }
    [DbConfigurationType(typeof(Discriminator.DAL.Myconfiguration))]
    public class EfDbContext : DbContext{
        public EfDbContext() : base("Name=DiscriminatorConn") {
            // Database.SetInitializer<EfDbContext>(null);
            //使用DbConfiguration后这里就不用配置了
            //  Database.SetInitializer( new DropCreateDatabaseIfModelChanges<EfDbContext>());
        }

        //使用dbset来显露基类属性
        public DbSet<BillingDetail> BillingDetails { get; set; }

        protected override void OnModelCreating(DbModelBuilder modelBuilder) {
            //自定义Discriminator值,对继承层次的类型进行区分（不同类型的返回值不同）
            //没有自定义的话系统会在表中自动加一个Discriminator列区分 ，这里自定义了所加BillingDetailType
            modelBuilder.Entity<BillingDetail>().Map<BankAccount>(m => m.Requires("BillingDetailType").HasValue(1))
                    .Map<CreditCard>(m => m.Requires("BillingDetailType").HasValue(2));
        }
    }

}