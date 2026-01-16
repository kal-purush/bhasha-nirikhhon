using FootyTips.Data.Models;
using Microsoft.EntityFrameworkCore;

namespace FootyTips.Data
{
    public class FootyTipsContext : DbContext
    {
        public DbSet<Competition> Competitions { get; set; }
        public DbSet<Season> Seasons { get; set; }
        public DbSet<Team> Teams { get; set; }
        public DbSet<Match> Matches { get; set; }
        public DbSet<TeamMatch> TeamMatches { get; set; }
        public DbSet<Referee> Referees { get; set; }

        public string DbPath { get; }

        public FootyTipsContext()
        {
            //appdata folder - change at later date to hosted db
            var folder = Environment.SpecialFolder.LocalApplicationData;
            var path = Environment.GetFolderPath(folder);
            DbPath = Path.Join(path, "FootyTips.db");
        }

        protected override void OnConfiguring(DbContextOptionsBuilder options)
            => options.UseSqlite($"Data Source={DbPath}");
    }
}