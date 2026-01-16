using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.ModelConfiguration.Conventions;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Entity.Validation;
using Models;
using System.Data.Common;
using System.Threading;
using System.Runtime.Remoting.Contexts;

namespace DataContext
{
    public class SimpleLibraryDbContext : DbContext
    {
        public SimpleLibraryDbContext()
            : base("DefaultConnection")
        {
            this.Configuration.LazyLoadingEnabled = false;
            SetDefaultConfiguration();
        }
        public SimpleLibraryDbContext(DbConnection existingConnection)
            : base(existingConnection, true)
        {
            SetDefaultConfiguration();
        }

        //public override int SaveChanges()
        //{
        //    SetStudentId();
        //    return base.SaveChanges();
        //}

        //public override Task<int> SaveChangesAsync()
        //{
        //    SetStudentId();
        //    return base.SaveChangesAsync();
        //}

        //public override Task<int> SaveChangesAsync(CancellationToken cancellationToken)
        //{
        //    SetStudentId();
        //    return base.SaveChangesAsync(cancellationToken);
        //}

        private void SetDefaultConfiguration()
        {
            Database.SetInitializer(new CreateSequenceInitializer());
        }

        public class CreateSequenceInitializer : DropCreateDatabaseIfModelChanges<SimpleLibraryDbContext>
        {
            protected override void Seed(SimpleLibraryDbContext context)
            {
                context.Database.ExecuteSqlCommand("CREATE SEQUENCE StudentSequence AS INT START WITH "+DateTime.Now.Year+"000001 "  +"NO CACHE;");
                base.Seed(context);
            }
        }

        //private void SetStudentId()
        //{
        //    var studentsToSave = ChangeTracker.Entries().Select(e => e.Entity).OfType<Student>();
        //    foreach (var c in studentsToSave)
        //    {
        //        c.StudentId = GetNextSequenceValue();
        //    }
        //}

        public int GetNextSequenceValue()
        {
            var rawQuery = Database.SqlQuery<int>("SELECT NEXT VALUE FOR StudentSequence;");
            var task = rawQuery.SingleAsync();
            int nextVal = task.Result;
            return nextVal;
        }


        public DbSet<Student> Students { get; set; }
        public DbSet<Book> Books { get; set; }
        public DbSet<BookIssueMain> BookIssueMains { get; set; }
        public DbSet<BookIssueDetail> BookIssueDetails { get; set; }

        //public DbSet<UnuseableSequence> UnuseableSequences { get; set; }
        //public DbSet<Logs> Logs { get; set; }
        //public DbSet<LogType> LogTypes { get; set; }

        #region Overridden Methods
        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BookIssueDetail>()
                        .HasRequired<BookIssueMain>(s => s.BookIssueMain)
                        .WithMany(s => s.BookIssueDetails);

        }
        #endregion
    }
}