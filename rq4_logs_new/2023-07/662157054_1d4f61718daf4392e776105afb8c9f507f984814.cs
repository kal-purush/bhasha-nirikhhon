using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using cvb.dal.Models;

namespace cvb.dal.Data;

public partial class CvbuilderContext : DbContext
{
    public CvbuilderContext()
    {
    }

    public CvbuilderContext(DbContextOptions<CvbuilderContext> options)
        : base(options)
    {
    }

    public virtual DbSet<Cv> Cvs { get; set; }

    public virtual DbSet<User> Users { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
#warning To protect potentially sensitive information in your connection string, you should move it out of source code. You can avoid scaffolding the connection string by using the Name= syntax to read it from configuration - see https://go.microsoft.com/fwlink/?linkid=2131148. For more guidance on storing connection strings, see http://go.microsoft.com/fwlink/?LinkId=723263.
        => optionsBuilder.UseSqlServer("data source = .\\SQLEXPRESS; initial catalog=CVBuilder; Encrypt=false; Trusted_Connection=true;");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Cv>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__CV__3214EC07FD9862B2");

            entity.HasOne(d => d.User).WithMany(p => p.Cvs).HasConstraintName("FK__CV__UserId__398D8EEE");
        });

        modelBuilder.Entity<User>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__Users__3214EC07CAAEB678");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}