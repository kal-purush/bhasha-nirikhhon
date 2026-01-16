using Microsoft.EntityFrameworkCore;
using StudyBuddy.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace StudyBuddy
{
  class StudyBuddyContext : DbContext
  {
    public DbSet<Question> Questions { get; set; }
    public DbSet<Favorite> Favorites { get; set; }
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
      optionsBuilder.UseSqlServer("Server=.\\SQLExpress;Database=StudyBuddy;Trusted_Connection=True;");
    }
  }
}