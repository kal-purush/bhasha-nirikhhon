using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using CourseGenerator.Models;
using CourseGenerator.Models.Entities.Info;
using CourseGenerator.Models.Entities.InfoByThemes;
using CourseGenerator.Models.Entities.CourseAccess;

namespace CourseGenerator.DAL.Context
{
    public class Context : IdentityDbContext
    {
        private readonly string _connectionString;

        #region Info
        public DbSet<Language> Languages;
        public DbSet<Heading> Headings;
        public DbSet<HeadingLang> HeadingLangs;
        public DbSet<Level> Levels;
        public DbSet<LevelLang> LevelLangs;
        public DbSet<Competency> Competencies;
        public DbSet<CompetencyLang> CompetencyLangs;
        public DbSet<HeadingCompetency> HeadingCompetencies;
        public DbSet<MaterialType> MaterialTypes;
        public DbSet<MaterialTypeLang> MaterialTypeLangs;
        public DbSet<Material> Materials;
        public DbSet<MaterialLang> MaterialLangs;
        public DbSet<MaterialCompetency> MaterialCompetencies;
        public DbSet<HeadingMaterial> HeadingMaterials;
        #endregion

        #region InfoByThemes
        public DbSet<Course> Courses;
        public DbSet<CourseLang> CourseLangs;
        public DbSet<CourseDependency> CourseDependencies;
        public DbSet<Theme> Themes;
        public DbSet<ThemeLang> ThemeLangs;
        public DbSet<CourseHeading> CourseHeadings;
        public DbSet<ThemeHeading> ThemeHeadings;
        public DbSet<ThemeMaterial> ThemeMaterials;
        #endregion

        #region CourseAccess
        public DbSet<UserHeading> UserHeadings;
        public DbSet<UserCourse> UserCourses;
        public DbSet<UserTheme> UserThemes;
        #endregion


        public Context(string connectionString)
        {
            _connectionString = connectionString;

            Database.EnsureDeleted();
            Database.EnsureCreated();
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder.UseSqlServer(_connectionString);
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            #region Info block
            modelBuilder.Entity<Language>().HasData(TestData.Languages);
            modelBuilder.Entity<Heading>().HasData(TestData.Headings);
            modelBuilder.Entity<HeadingLang>().HasData(TestData.HeadingLangs);
            modelBuilder.Entity<Level>().HasData(TestData.Levels);
            modelBuilder.Entity<LevelLang>().HasData(TestData.LevelLangs);
            modelBuilder.Entity<Competency>().HasData(TestData.Competencies);
            modelBuilder.Entity<CompetencyLang>().HasData(TestData.CompetencyLangs);
            modelBuilder.Entity<HeadingCompetency>().HasData(TestData.HeadingCompetencies);
            modelBuilder.Entity<MaterialType>().HasData(TestData.MaterialTypes);
            modelBuilder.Entity<MaterialTypeLang>().HasData(TestData.MaterialTypeLangs);
            modelBuilder.Entity<Material>().HasData(TestData.Materials);
            modelBuilder.Entity<MaterialLang>().HasData(TestData.MaterialLangs);
            modelBuilder.Entity<MaterialCompetency>().HasData(TestData.MaterialCompetencies);
            modelBuilder.Entity<HeadingMaterial>().HasData(TestData.HeadingMaterials);
            #endregion

            #region InfoByThemes
            modelBuilder.Entity<Course>().HasData(TestData.Courses);
            modelBuilder.Entity<CourseLang>().HasData(TestData.CourseLangs);
            modelBuilder.Entity<CourseDependency>().HasData(TestData.CourseDependencies);
            modelBuilder.Entity<Theme>().HasData(TestData.Themes);
            modelBuilder.Entity<ThemeLang>().HasData(TestData.ThemeLangs);
            modelBuilder.Entity<CourseHeading>().HasData(TestData.CourseHeadings);
            modelBuilder.Entity<ThemeHeading>().HasData(TestData.ThemeHeadings);
            modelBuilder.Entity<ThemeMaterial>().HasData(TestData.ThemeMaterials);
            #endregion

            #region CourseAccess
            modelBuilder.Entity<UserHeading>().HasData(TestData.UserHeadings);
            modelBuilder.Entity<UserCourse>().HasData(TestData.UserCourses);
            modelBuilder.Entity<UserTheme>().HasData(TestData.UserThemes);
            #endregion
        }
    }
}