using Microsoft.EntityFrameworkCore;
using EtudeManyToMany.Core.Model;

namespace EtudeManyToMany.API.Data
{
    public class ApplicationDbContext : DbContext
    {
        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options) : base(options)
        {
        }

        public DbSet<Utilisateur> Utilisateurs { get; set; }
        public DbSet<Trajet> Trajets { get; set; }
        public DbSet<Reservation> Reservations { get; set; }
        public DbSet<Conducteur> Conducteurs { get; set; }
        public DbSet<Passager> Passagers { get; set; }
        public DbSet<Administrateur> Admins { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Utilisateur>()
                .HasOne(u => u.Conducteur)
                .WithOne(c => c.Utilisateur)
                .HasForeignKey<Conducteur>(c => c.UtilisateurId);

            modelBuilder.Entity<Utilisateur>()
                .HasOne(u => u.Passager)
                .WithOne(p => p.Utilisateur)
                .HasForeignKey<Passager>(p => p.UtilisateurId);

            modelBuilder.Entity<Conducteur>()
                .HasMany(c => c.Trajets)
                .WithOne(t => t.Conducteur)
                .HasForeignKey(t => t.ConducteurId);

            modelBuilder.Entity<Passager>()
                .HasMany(p => p.Reservations)
                .WithOne(r => r.Passager)
                .HasForeignKey(r => r.PassagerId);

            modelBuilder.Entity<Trajet>()
                .HasMany(t => t.Reservations)
                .WithOne(r => r.Trajet)
                .HasForeignKey(r => r.TrajetId);
        }
    }
}