using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Shared;

namespace Backend.EFCData;

public class DataContext : DbContext
{

    public DbSet<Movie>? Movies { get; set; } = null;
    public DbSet<User>? Users { get; set; } = null;
    public DbSet<Person>? Persons { get; set; } = null;
    public DbSet<Director>? Directors { get; set; } = null;
    public DbSet<Star>? Stars { get; set; } = null;
    public DbSet<Rating>? Ratings { get; set; } = null;
    public DbSet<UserRating>? UserRatings { get; set; } = null;
    public DbSet<Favourite>? Favourites { get; set; } = null;
    
    protected readonly IConfiguration Configuration;

    public DataContext(IConfiguration configuration)
    {
        Configuration = configuration;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        
        // connect to postgres with connection string from app settings
        options.UseNpgsql(Configuration.GetConnectionString("BestMoviesDB"));
       
    }

     protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Movie
        modelBuilder.Entity<Movie>()
            .HasKey(m => m.Id);

        modelBuilder.Entity<Movie>()
            .HasOne(m => m.Rating)
            .WithOne(r => r.Movie)
            .HasForeignKey<Rating>(r => r.MovieId);

       // Star
        modelBuilder.Entity<Star>()
            .HasKey(s => new { s.MovieId, s.PersonId });
       
        modelBuilder.Entity<Star>()
            .HasOne(s => s.Movie)
            .WithMany(m => m.Stars)
            .HasForeignKey(s => s.MovieId);

        modelBuilder.Entity<Star>()
            .HasOne(s => s.Person)
            .WithMany(p => p.StarredMovies)
            .HasForeignKey(s => s.PersonId);

        // Director
        modelBuilder.Entity<Director>()
            .HasKey(d => new { d.MovieId, d.PersonId });

        modelBuilder.Entity<Director>()
            .HasOne(d => d.Movie)
            .WithMany(m => m.Directors)
            .HasForeignKey(d => d.MovieId);

        modelBuilder.Entity<Director>()
            .HasOne(d => d.Person)
            .WithMany(p => p.DirectedMovies)
            .HasForeignKey(d => d.PersonId);

        // Rating
        modelBuilder.Entity<Rating>()
            .HasKey(r => r.MovieId);

        // Person
        modelBuilder.Entity<Person>()
            .HasKey(p => p.Id);

        // User
        modelBuilder.Entity<User>()
            .HasKey(u => u.Id);

        // Favourite
        modelBuilder.Entity<Favourite>()
            .HasKey(f => new { f.MovieId, f.UserId });

        modelBuilder.Entity<Favourite>()
            .HasOne(f => f.Movie)
            .WithMany(m => m.Favourites)
            .HasForeignKey(f => f.MovieId);

        modelBuilder.Entity<Favourite>()
            .HasOne(f => f.User)
            .WithMany(u => u.Favourites)
            .HasForeignKey(f => f.UserId);

        // UserRating
        modelBuilder.Entity<UserRating>()
            .HasKey(ur => new { ur.MovieId, ur.UserId });

        modelBuilder.Entity<UserRating>()
            .HasOne(ur => ur.Movie)
            .WithMany(m => m.UserRatings)
            .HasForeignKey(ur => ur.MovieId);

        modelBuilder.Entity<UserRating>()
            .HasOne(ur => ur.User)
            .WithMany(u => u.UserRatings)
            .HasForeignKey(ur => ur.UserId);
    }
}
    