using System;
using System.Collections.Generic;
using KriaMVC.Models;
using Microsoft.EntityFrameworkCore;

namespace KriaMVC.Context;

public partial class KriaContext : DbContext
{
    public KriaContext()
    {
    }

    public KriaContext(DbContextOptions<KriaContext> options)
        : base(options)
    {
    }

    public virtual DbSet<Favorito> Favoritos { get; set; }

    public virtual DbSet<Repositorio> Repositorios { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
#warning To protect potentially sensitive information in your connection string, you should move it out of source code. You can avoid scaffolding the connection string by using the Name= syntax to read it from configuration - see https://go.microsoft.com/fwlink/?linkid=2131148. For more guidance on storing connection strings, see http://go.microsoft.com/fwlink/?LinkId=723263.
        => optionsBuilder.UseSqlServer("Server=DESKTOP-81POI2L\\SQLEXPRESS;Database=KRIA;Trusted_Connection=True;Encrypt=False");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Favorito>(entity =>
        {
            entity.HasKey(e => e.FavId).HasName("PK__Favorito__9694C495D383E994");

            entity.ToTable("Favorito");

            entity.HasOne(d => d.Repo).WithMany(p => p.Favoritos)
                .HasForeignKey(d => d.RepoId)
                .HasConstraintName("FK__Favorito__RepoId__5EBF139D");
        });

        modelBuilder.Entity<Repositorio>(entity =>
        {
            entity.HasKey(e => e.RepoId).HasName("PK__Reposito__85680678A81A1EF8");

            entity.ToTable("Repositorio");

            entity.Property(e => e.Descricao)
                .HasMaxLength(500)
                .IsUnicode(false);
            entity.Property(e => e.DonoRepositorio)
                .HasMaxLength(20)
                .IsUnicode(false);
            entity.Property(e => e.Linguagem)
                .HasMaxLength(500)
                .IsUnicode(false);
            entity.Property(e => e.Nome)
                .HasMaxLength(100)
                .IsUnicode(false);
            entity.Property(e => e.UltimaAtt).HasColumnType("datetime");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}