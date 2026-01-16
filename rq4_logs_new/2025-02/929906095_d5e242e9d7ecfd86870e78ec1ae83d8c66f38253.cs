using BusinessObject.Entities;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

namespace DataAccess.Context
{
    public class ApplicationDbContext(DbContextOptions<ApplicationDbContext> options) : DbContext(options)
    {
        public DbSet<User> Users { get; set; }
        public DbSet<Role> Roles { get; set; }
        public DbSet<EmailConfirmationToken> EmailConfirmationTokens { get; set; }
        public DbSet<RefreshToken> RefreshTokens { get; set; }
        public DbSet<Voucher> Voucher { get; set; }
        public DbSet<UserVoucher> UserVouchers { get; set; }
        public DbSet<PostImage> PostImages { get; set; }
        public DbSet<Post> Posts { get; set; }
        public DbSet<HomeStayImage> HomeStayImages { get; set; }
        public DbSet<HomestayAmenity>HomestayAmenities { get; set; }
        public DbSet<HomeStay> HomeStays { get; set; }
        public DbSet<FeedBack> FeedBacks { get; set; }
        public DbSet<CommentPost> CommentPosts { get; set; }
        public DbSet<Calendar> Calendars {  get; set; }
        public DbSet<Booking> Bookings { get; set; }
        public DbSet<Amenity> Amennities {  get; set; } 
        
        protected override void OnModelCreating(ModelBuilder builder)
        {
            base.OnModelCreating(builder);

            builder.Entity<UserVoucher>().HasKey(u => new { u.UserID, u.VoucherID });
            builder.Entity<HomestayAmenity>().HasKey(ha => new { ha.HomeStayID, ha.AmenityId });
              
            builder.Entity<Booking>()
            .Property(b => b.TotalPrice)
            .HasPrecision(18, 4);

            builder.Entity<Booking>()
                .Property(b => b.UnitPrice)
            .HasPrecision(18, 4);

            builder.Entity<Calendar>()
                .Property(c => c.Price)
            .HasPrecision(18, 4);

            base.OnModelCreating(builder);
            builder.Entity<Role>()
                .HasData(
                    new Role() { Id = 1, Name = "Admin" },
                    new Role() { Id = 2, Name = "Manager" },
                    new Role() { Id = 3, Name = "User" }
                );

            Guid adminId = new("d87b4b72-609b-4979-b758-7771481da883");
            Guid staffId = new("4b7b0200-70f9-416a-9a3f-29ccab0deec4");
            Guid userId = new("a85f272f-353e-4ff6-be2b-a15f1e7c0c47");

            builder.Entity<User>()
                .HasData(
                    new User()
                    {
                        Id = adminId,
                        Email = "admin@gmail.com",
                        PasswordHash = BCrypt.Net.BCrypt.HashPassword("aA@123"),
                        FullName = "admin",
                        Address = "Ninh Kiều, Cần Thơ",
                        Phone = "0987654321",
                        IsEmailConfirmed = true,
                        RoleId = 1
                    },
                    new User()
                    {
                        Id = staffId,
                        Email = "staff@gmail.com",
                        PasswordHash = BCrypt.Net.BCrypt.HashPassword("aA@123"),
                        FullName = "staff",
                        Address = "Bình Thủy, Cần Thơ",
                        Phone = "0987654123",
                        IsEmailConfirmed = true,
                        RoleId = 2
                    },
                    new User()
                    {
                        Id = userId,
                        Email = "user@gmail.com",
                        PasswordHash = BCrypt.Net.BCrypt.HashPassword("aA@123"),
                        FullName = "user",
                        Address = "Phong Điền, Cần Thơ",
                        Phone = "0987654312",
                        IsEmailConfirmed = true,
                        RoleId = 3
                    }
            );
        }
    }
}