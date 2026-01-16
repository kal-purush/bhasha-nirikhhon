using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data.Entity;
using QMaze.Models;
using System.Data.Entity.ModelConfiguration.Conventions;

namespace QMaze.DataAccessLayer
{
    public class QuestionContext : DbContext
    {
        public QuestionContext() : base("QuestionContext") { }

        public DbSet<Question> Questions { get; set; }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            modelBuilder.Conventions.Remove<PluralizingTableNameConvention>();
            base.OnModelCreating(modelBuilder);
        }
    }
}