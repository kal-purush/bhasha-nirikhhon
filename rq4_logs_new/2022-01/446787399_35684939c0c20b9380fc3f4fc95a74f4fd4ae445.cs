using System;
using System.IO;
using Microsoft.EntityFrameworkCore;
using TaskDudes.Models;
using Xamarin.Essentials;
using Xamarin.Forms;

namespace TaskDudes.Services
{
    public class TaskMatesTestContext : DbContext
    {

        public DbSet<User> Users { get; set; }
        public DbSet<Taski> Tasks { get; set; }
        public DbSet<Settings> Settings { get; set; }


        private const string DatabaseName = "taskMatesTestData.db";

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            String databasePath;
            switch (Device.RuntimePlatform)
            {
                case Device.iOS:
                    databasePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "..", "Library", DatabaseName);
                    break;
                case Device.Android:
                    databasePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Personal), DatabaseName);
                    break;
                default:
                    throw new NotImplementedException("Platform not supported");
            }
            optionsBuilder.UseSqlite($"Filename={databasePath}");
        }
    }
}
