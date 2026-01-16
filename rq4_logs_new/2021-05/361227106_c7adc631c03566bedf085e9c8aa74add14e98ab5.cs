using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

#nullable disable

namespace TAB_clinic_GUI.Database
{
    public partial class ClinicDBContext : DbContext
    {
        public ClinicDBContext()
        {
        }

        public ClinicDBContext(DbContextOptions<ClinicDBContext> options)
            : base(options)
        {
        }

        public virtual DbSet<Doctor> Doctors { get; set; }
        public virtual DbSet<ExamType> ExamTypes { get; set; }
        public virtual DbSet<LabExam> LabExams { get; set; }
        public virtual DbSet<LabManager> LabManagers { get; set; }
        public virtual DbSet<LabWorker> LabWorkers { get; set; }
        public virtual DbSet<Patient> Patients { get; set; }
        public virtual DbSet<PhysicalExam> PhysicalExams { get; set; }
        public virtual DbSet<Registar> Registars { get; set; }
        public virtual DbSet<User> Users { get; set; }
        public virtual DbSet<Visit> Visits { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
#warning To protect potentially sensitive information in your connection string, you should move it out of source code. You can avoid scaffolding the connection string by using the Name= syntax to read it from configuration - see https://go.microsoft.com/fwlink/?linkid=2131148. For more guidance on storing connection strings, see http://go.microsoft.com/fwlink/?LinkId=723263.
                optionsBuilder.UseSqlServer("Server=localhost;Database=ClinicDB;Trusted_Connection=True;");
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasAnnotation("Relational:Collation", "SQL_Latin1_General_CP1_CI_AS");

            modelBuilder.Entity<Doctor>(entity =>
            {
                entity.HasKey(e => e.IdUser)
                    .HasName("PK_doctor");

                entity.ToTable("Doctor");

                entity.Property(e => e.IdUser)
                    .ValueGeneratedNever()
                    .HasColumnName("id_user");

                entity.HasOne(d => d.IdUserNavigation)
                    .WithOne(p => p.Doctor)
                    .HasForeignKey<Doctor>(d => d.IdUser)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_18");
            });

            modelBuilder.Entity<ExamType>(entity =>
            {
                entity.HasKey(e => e.Code)
                    .HasName("PK_exam_type");

                entity.ToTable("Exam_type");

                entity.Property(e => e.Code)
                    .HasMaxLength(5)
                    .IsUnicode(false)
                    .HasColumnName("code");

                entity.Property(e => e.Name)
                    .IsRequired()
                    .HasMaxLength(50)
                    .HasColumnName("name");

                entity.Property(e => e.Type)
                    .IsRequired()
                    .HasMaxLength(1)
                    .IsUnicode(false)
                    .HasColumnName("type")
                    .IsFixedLength(true);
            });

            modelBuilder.Entity<LabExam>(entity =>
            {
                entity.HasKey(e => e.IdLabExam)
                    .HasName("PK_lab_exam");

                entity.ToTable("Lab_exam");

                entity.Property(e => e.IdLabExam).HasColumnName("id_lab_exam");

                entity.Property(e => e.Code)
                    .IsRequired()
                    .HasMaxLength(5)
                    .IsUnicode(false)
                    .HasColumnName("code");

                entity.Property(e => e.DoctorsNotes)
                    .HasColumnType("ntext")
                    .HasColumnName("doctors_notes");

                entity.Property(e => e.DtApproved)
                    .HasColumnType("datetime")
                    .HasColumnName("dt_approved");

                entity.Property(e => e.DtFinalized)
                    .HasColumnType("datetime")
                    .HasColumnName("dt_finalized");

                entity.Property(e => e.DtRequest)
                    .HasColumnType("datetime")
                    .HasColumnName("dt_request");

                entity.Property(e => e.IdManager).HasColumnName("id_manager");

                entity.Property(e => e.IdVisit).HasColumnName("id_visit");

                entity.Property(e => e.IdWorker).HasColumnName("id_worker");

                entity.Property(e => e.ManagersNotes)
                    .HasColumnType("ntext")
                    .HasColumnName("managers_notes");

                entity.Property(e => e.Result)
                    .HasColumnType("ntext")
                    .HasColumnName("result");

                entity.Property(e => e.Status)
                    .IsRequired()
                    .HasMaxLength(2)
                    .IsUnicode(false)
                    .HasColumnName("status");

                entity.HasOne(d => d.CodeNavigation)
                    .WithMany(p => p.LabExams)
                    .HasForeignKey(d => d.Code)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_43");

                entity.HasOne(d => d.IdManagerNavigation)
                    .WithMany(p => p.LabExams)
                    .HasForeignKey(d => d.IdManager)
                    .HasConstraintName("FK_35");

                entity.HasOne(d => d.IdVisitNavigation)
                    .WithMany(p => p.LabExams)
                    .HasForeignKey(d => d.IdVisit)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_29");

                entity.HasOne(d => d.IdWorkerNavigation)
                    .WithMany(p => p.LabExams)
                    .HasForeignKey(d => d.IdWorker)
                    .HasConstraintName("FK_32");
            });

            modelBuilder.Entity<LabManager>(entity =>
            {
                entity.HasKey(e => e.IdUser)
                    .HasName("PK_lab_manager");

                entity.ToTable("Lab_manager");

                entity.Property(e => e.IdUser)
                    .ValueGeneratedNever()
                    .HasColumnName("id_user");

                entity.HasOne(d => d.IdUserNavigation)
                    .WithOne(p => p.LabManager)
                    .HasForeignKey<LabManager>(d => d.IdUser)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_68");
            });

            modelBuilder.Entity<LabWorker>(entity =>
            {
                entity.HasKey(e => e.IdUser)
                    .HasName("PK_lab_worker");

                entity.ToTable("Lab_worker");

                entity.Property(e => e.IdUser)
                    .ValueGeneratedNever()
                    .HasColumnName("id_user");

                entity.HasOne(d => d.IdUserNavigation)
                    .WithOne(p => p.LabWorker)
                    .HasForeignKey<LabWorker>(d => d.IdUser)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_63");
            });

            modelBuilder.Entity<Patient>(entity =>
            {
                entity.HasKey(e => e.IdPatient)
                    .HasName("PK_patient");

                entity.ToTable("Patient");

                entity.Property(e => e.IdPatient).HasColumnName("id_patient");

                entity.Property(e => e.Lastname)
                    .IsRequired()
                    .HasMaxLength(50)
                    .HasColumnName("lastname");

                entity.Property(e => e.Name)
                    .IsRequired()
                    .HasMaxLength(50)
                    .HasColumnName("name");

                entity.Property(e => e.Pesel)
                    .IsRequired()
                    .HasMaxLength(11)
                    .IsUnicode(false)
                    .HasColumnName("PESEL");
            });

            modelBuilder.Entity<PhysicalExam>(entity =>
            {
                entity.HasKey(e => e.IdPhysicalExam)
                    .HasName("PK_physical_exam");

                entity.ToTable("Physical_exam");

                entity.Property(e => e.IdPhysicalExam).HasColumnName("id_physical_exam");

                entity.Property(e => e.Code)
                    .IsRequired()
                    .HasMaxLength(5)
                    .IsUnicode(false)
                    .HasColumnName("code");

                entity.Property(e => e.IdVisit).HasColumnName("id_visit");

                entity.Property(e => e.Result)
                    .HasColumnType("ntext")
                    .HasColumnName("result");

                entity.HasOne(d => d.CodeNavigation)
                    .WithMany(p => p.PhysicalExams)
                    .HasForeignKey(d => d.Code)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_46");

                entity.HasOne(d => d.IdVisitNavigation)
                    .WithMany(p => p.PhysicalExams)
                    .HasForeignKey(d => d.IdVisit)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_26");
            });

            modelBuilder.Entity<Registar>(entity =>
            {
                entity.HasKey(e => e.IdUser)
                    .HasName("PK_registar");

                entity.ToTable("Registar");

                entity.Property(e => e.IdUser)
                    .ValueGeneratedNever()
                    .HasColumnName("id_user");

                entity.HasOne(d => d.IdUserNavigation)
                    .WithOne(p => p.Registar)
                    .HasForeignKey<Registar>(d => d.IdUser)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_10");
            });

            modelBuilder.Entity<User>(entity =>
            {
                entity.HasKey(e => e.IdUser)
                    .HasName("PK_user");

                entity.ToTable("User");

                entity.HasIndex(e => e.Login, "UK_login")
                    .IsUnique();

                entity.Property(e => e.IdUser).HasColumnName("id_user");

                entity.Property(e => e.Login)
                    .IsRequired()
                    .HasMaxLength(50)
                    .IsUnicode(false)
                    .HasColumnName("login");

                entity.Property(e => e.Password)
                    .IsRequired()
                    .HasMaxLength(50)
                    .IsUnicode(false)
                    .HasColumnName("password");

                entity.Property(e => e.Role)
                    .HasMaxLength(5)
                    .IsUnicode(false)
                    .HasColumnName("role");
            });

            modelBuilder.Entity<Visit>(entity =>
            {
                entity.HasKey(e => e.IdVisit)
                    .HasName("PK_visit");

                entity.ToTable("Visit");

                entity.Property(e => e.IdVisit).HasColumnName("id_visit");

                entity.Property(e => e.Diagnosis)
                    .HasColumnType("ntext")
                    .HasColumnName("diagnosis");

                entity.Property(e => e.DtFinalized)
                    .HasColumnType("datetime")
                    .HasColumnName("dt_finalized");

                entity.Property(e => e.DtRegistered)
                    .HasColumnType("datetime")
                    .HasColumnName("dt_registered");

                entity.Property(e => e.IdDoctor).HasColumnName("id_doctor");

                entity.Property(e => e.IdPatient).HasColumnName("id_patient");

                entity.Property(e => e.IdRegistar).HasColumnName("id_registar");

                entity.Property(e => e.Status)
                    .IsRequired()
                    .HasMaxLength(1)
                    .IsUnicode(false)
                    .HasColumnName("status")
                    .IsFixedLength(true);

                entity.HasOne(d => d.IdDoctorNavigation)
                    .WithMany(p => p.Visits)
                    .HasForeignKey(d => d.IdDoctor)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_61");

                entity.HasOne(d => d.IdPatientNavigation)
                    .WithMany(p => p.Visits)
                    .HasForeignKey(d => d.IdPatient)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_70");

                entity.HasOne(d => d.IdRegistarNavigation)
                    .WithMany(p => p.Visits)
                    .HasForeignKey(d => d.IdRegistar)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_48");
            });

            OnModelCreatingPartial(modelBuilder);
        }

        partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
    }
}