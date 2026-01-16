using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using rb.dal.Models;

namespace rb.dal.Data;

public partial class ResumeBuilderContext : DbContext
{
    public ResumeBuilderContext()
    {
    }

    public ResumeBuilderContext(DbContextOptions<ResumeBuilderContext> options)
        : base(options)
    {
    }

    public virtual DbSet<Certificate> Certificates { get; set; }

    public virtual DbSet<Education> Educations { get; set; }

    public virtual DbSet<EducationSkill> EducationSkills { get; set; }

    public virtual DbSet<Language> Languages { get; set; }

    public virtual DbSet<Resume> Resumes { get; set; }

    public virtual DbSet<Skill> Skills { get; set; }

    public virtual DbSet<Template> Templates { get; set; }

    public virtual DbSet<User> Users { get; set; }

    public virtual DbSet<UserLanguage> UserLanguages { get; set; }

    public virtual DbSet<WorkExperience> WorkExperiences { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
#warning To protect potentially sensitive information in your connection string, you should move it out of source code. You can avoid scaffolding the connection string by using the Name= syntax to read it from configuration - see https://go.microsoft.com/fwlink/?linkid=2131148. For more guidance on storing connection strings, see http://go.microsoft.com/fwlink/?LinkId=723263.
        => optionsBuilder.UseSqlServer("data source = .\\SQLEXPRESS; initial catalog=ResumeBuilder; Encrypt=false; Trusted_Connection=true;");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Certificate>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__Certific__3214EC07732AD411");
        });

        modelBuilder.Entity<Education>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__Educatio__3214EC07D00699C4");
        });

        modelBuilder.Entity<EducationSkill>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__Educatio__3214EC07F37156CF");

            entity.HasOne(d => d.Education).WithMany(p => p.EducationSkills)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("FK__Education__Educa__44FF419A");

            entity.HasOne(d => d.Skill).WithMany(p => p.EducationSkills)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("FK__Education__Skill__45F365D3");
        });

        modelBuilder.Entity<Language>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__Language__3214EC071ADA6BCD");
        });

        modelBuilder.Entity<Resume>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__Resumes__3214EC07CA2EE47C");

            entity.HasOne(d => d.Template).WithMany(p => p.Resumes)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("FK__Resumes__Templat__4D94879B");

            entity.HasOne(d => d.User).WithMany(p => p.Resumes)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("FK__Resumes__UserId__4CA06362");
        });

        modelBuilder.Entity<Skill>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__Skills__3214EC0750023C2D");
        });

        modelBuilder.Entity<Template>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__Template__3214EC07B61EE449");
        });

        modelBuilder.Entity<User>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__Users__3214EC07B5B59096");
        });

        modelBuilder.Entity<UserLanguage>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__UserLang__3214EC07D4C532A8");

            entity.HasOne(d => d.Language).WithMany(p => p.UserLanguages)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("FK__UserLangu__Langu__3C69FB99");

            entity.HasOne(d => d.User).WithMany(p => p.UserLanguages)
                .OnDelete(DeleteBehavior.ClientSetNull)
                .HasConstraintName("FK__UserLangu__UserI__3B75D760");
        });

        modelBuilder.Entity<WorkExperience>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PK__WorkExpe__3214EC072E8A5DB2");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}