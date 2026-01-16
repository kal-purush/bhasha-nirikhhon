using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

// Code scaffolded by EF Core assumes nullable reference types (NRTs) are not used or disabled.
// If you have enabled NRTs for your project, then un-comment the following line:
// #nullable disable

namespace HostelWebAPI.Models
{
    public partial class HostelDBContext : IdentityDbContext<User>
    {
        public HostelDBContext()
        {
        }

        public HostelDBContext(DbContextOptions<HostelDBContext> options)
            : base(options)
        {
        }

        public virtual DbSet<City> City { get; set; }
        public virtual DbSet<Comment> Comment { get; set; }
        public virtual DbSet<Country> Country { get; set; }
        public virtual DbSet<Image> Image { get; set; }
        public virtual DbSet<PaymentStatus> PaymentStatus { get; set; }
        public virtual DbSet<Property> Property { get; set; }
        public virtual DbSet<PropertyAddress> PropertyAddress { get; set; }
        public virtual DbSet<PropertyType> PropertyType { get; set; }
        public virtual DbSet<Reply> Reply { get; set; }
        public virtual DbSet<ReservationHistory> ReservationHistory { get; set; }
        public virtual DbSet<ReservationStatus> ReservationStatus { get; set; }
        public virtual DbSet<Owner> Owners { get; set; }
        public virtual DbSet<UserPropertyLike> UserPropertyLikes { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<User>(entity =>
            {
                entity.HasMany(e => e.Comments)
                .WithOne(e => e.User)
                .HasForeignKey(e => e.UserId)
                .OnDelete(DeleteBehavior.Cascade);

                entity.HasMany(u => u.UserPropertyLikes)
                .WithOne(upl => upl.User).OnDelete(DeleteBehavior.Cascade);

                //entity.HasMany(e => e.UserPropertyLikes)
                //.WithOne(upl => upl.User).HasForeignKey(upl => upl.UserId).OnDelete(DeleteBehavior.Cascade);
            });

            modelBuilder.Entity<Owner>(owner =>
            {
                owner.HasMany(o => o.Properties).WithOne(p => p.Owner).HasForeignKey(o => o.OwnerId).OnDelete(DeleteBehavior.Cascade);
            });

            modelBuilder.Entity<City>(entity =>
            {
                entity.HasOne(d => d.Country)
                    .WithMany(p => p.Cities)
                    .HasForeignKey(d => d.CountryId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_Cities_Countries");
            });

            modelBuilder.Entity<Comment>(entity =>
            {
                entity.Property(e => e.CommentString).HasDefaultValueSql("(N'Default comment')");

                entity.Property(e => e.TimeCreated).HasDefaultValueSql("(getdate())");

                entity.Property(e => e.TimeUpdated).HasDefaultValueSql("(getdate())");

                entity.HasOne(d => d.Property)
                    .WithMany(p => p.Comments)
                    .HasForeignKey(d => d.PropertyId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_Comments_Properties");
            });

            modelBuilder.Entity<Image>(entity =>
            {
                entity.Property(e => e.ImageId).IsUnicode(false);

                entity.HasOne(d => d.Property)
                    .WithMany(p => p.Images)
                    .HasForeignKey(d => d.PropertyId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_Images_Properties");
            });

            modelBuilder.Entity<PaymentStatus>(entity =>
            {
                entity.Property(e => e.PaymentStatusId).IsUnicode(false);
            });

            modelBuilder.Entity<Property>(entity =>
            {
                entity.Property(e => e.Description).HasDefaultValueSql("(N'Property Discription')");

                entity.Property(e => e.Introduction).HasDefaultValueSql("(N'Property Introduction')");

                entity.Property(e => e.Name).HasDefaultValueSql("(N'PropertyName')");

                entity.Property(e => e.PropertyTypeId).HasDefaultValueSql("((1))");

                entity.Property(e => e.Rating).HasDefaultValueSql("((5.0))");

                entity.Property(e => e.TimeCreated).HasDefaultValueSql("(getdate())");

                entity.Property(e => e.TimeUpdated).HasDefaultValueSql("(getdate())");

                entity.HasOne(d => d.PropertyType)
                    .WithMany(p => p.Properties)
                    .HasForeignKey(d => d.PropertyTypeId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_Properties_PropertyTypes");

                //
                //entity.HasMany(p => p.UserPropertyLikes)
                //.WithOne(upl => upl.Property).OnDelete(DeleteBehavior.Cascade);

                //entity.HasOne(e => e.PropertyAddress)
                //.WithOne(pa => pa.Property).OnDelete(DeleteBehavior.Cascade);

                //entity.HasMany(e => e.Comments)
                //.WithOne(cmt => cmt.Property).OnDelete(DeleteBehavior.Cascade);

                //entity.HasMany(e => e.Images)
                //.WithOne(img => img.Property).OnDelete(DeleteBehavior.Cascade);

                //entity.HasMany(e => e.ReservationHistories)
                //.WithOne(rsh => rsh.Property).OnDelete(DeleteBehavior.ClientSetNull);
            });

            modelBuilder.Entity<PropertyAddress>(entity =>
            {
                entity.HasKey(e => e.PropertyId)
                    .HasName("PK_RoomAddresses");

                entity.Property(e => e.Number)
                    .IsUnicode(false)
                    .HasDefaultValueSql("('Street number default')");

                entity.Property(e => e.StreetName).HasDefaultValueSql("(N'Street Name default')");

                entity.HasOne(d => d.City)
                    .WithMany(p => p.PropertyAddresses)
                    .HasForeignKey(d => d.CityId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_RoomAddresses_Cities");

                entity.HasOne(d => d.Property)
                    .WithOne(p => p.PropertyAddress)
                    .HasForeignKey<PropertyAddress>(d => d.PropertyId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_RoomAddresses_Properties");
            });

            modelBuilder.Entity<Reply>(entity =>
            {
                entity.HasKey(e => e.CommentId)
                    .HasName("PK_Replies");

                entity.HasOne(d => d.Comment)
                    .WithOne(p => p.ReplyComment)
                    .HasForeignKey<Reply>(d => d.CommentId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_Replies_Comments");

                entity.HasOne(d => d.ReplyToComment)
                    .WithMany(p => p.ReplyReplyToComments)
                    .HasForeignKey(d => d.ReplyToCommentId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_Replies_Comments1");
            });

            modelBuilder.Entity<ReservationHistory>(entity =>
            {
                entity.HasKey(e => e.ReservationId)
                    .HasName("PK_ReservationHistories");

                entity.Property(e => e.ReservationId).IsUnicode(false);

                entity.Property(e => e.UserId).HasColumnType("nvarchar(450)");

                entity.Property(e => e.FromDate).HasDefaultValueSql("(getdate())");

                entity.Property(e => e.PaymentStatusId)
                    .IsUnicode(false)
                    .HasDefaultValueSql("((1))");

                entity.Property(e => e.ReservationStatusId)
                    .IsUnicode(false)
                    .HasDefaultValueSql("((1))");

                entity.Property(e => e.TimeCreated).HasDefaultValueSql("(getdate())");

                entity.Property(e => e.ToDate).HasDefaultValueSql("(getdate())");

                entity.Property(e => e.UserId).IsUnicode(false);

                entity.HasOne(d => d.PaymentStatus)
                    .WithMany(p => p.ReservationHistories)
                    .HasForeignKey(d => d.PaymentStatusId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_ReservationHistories_PaymentStatus");

                entity.HasOne(d => d.Property)
                    .WithMany(p => p.ReservationHistories)
                    .HasForeignKey(d => d.PropertyId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_ReservationHistories_Properties");

                entity.HasOne(d => d.ReservationStatus)
                    .WithMany(p => p.ReservationHistories)
                    .HasForeignKey(d => d.ReservationStatusId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_ReservationHistories_ReservationStatus");
            });

            modelBuilder.Entity<ReservationStatus>(entity =>
            {
                entity.Property(e => e.ReservationStatusId).IsUnicode(false);
            });

            OnModelCreatingPartial(modelBuilder);
        }

        partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
    }
}