using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace PrototipoModeloPlataforma.Modelos
{
    public class Contexto :DbContext
    {
        public DbSet<Aula> Aulas { get; set; }
        public DbSet<Usuario> Usuarios { get; set; }
        public DbSet<Curso> Cursos { get; set; }


        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlite(connectionString: "Filename=" + "database.db",
                sqliteOptionsAction: op =>
                {
                    op.MigrationsAssembly(Assembly.GetExecutingAssembly().FullName);
                });
            base.OnConfiguring(optionsBuilder);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Aula>().ToTable("Aulas");
            modelBuilder.Entity<Aula>(entity => { entity.HasKey(e => new { e.id}); });
            modelBuilder.Entity<Usuario>().ToTable("Usuario");
            modelBuilder.Entity<Usuario>(entity => { entity.HasKey(e => new { e.id }); });
            modelBuilder.Entity<Curso>().ToTable("Curso");
            modelBuilder.Entity<Curso>(entity => { entity.HasKey(e => new { e.id }); });
        }

    }
}