using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Event_Planinng_System_DAL.Models
{
    public class dbContext : DbContext
    {
        public dbContext()
        {
        }
        public dbContext(DbContextOptions<dbContext> options) : base(options) { }

        public virtual DbSet<User> Users { get; set; }
        public virtual DbSet<Attendance> Attendances { get; set; }
        public virtual DbSet<Comments> Comments { get; set; }
        public virtual DbSet<Emails> Emails { get; set; }
        public virtual DbSet<Event> Events { get; set; }
        public virtual DbSet<EventImages> EventsImages { get; set; }
        public virtual DbSet<Invite> Invites { get; set; }
        public virtual DbSet<Role> Roles { get; set; }
        public virtual DbSet<ToDoList> ToDoLists { get; set; }
        public virtual DbSet<UserRole> UserRoles { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            base.OnConfiguring(optionsBuilder);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            //user
            modelBuilder.Entity<User>(entity =>
            {
                entity.HasIndex(e=>e.Email).IsUnique();
            });

            //comments
            modelBuilder.Entity<Comments>(entity =>
            {
                entity.HasKey(e => new { e.UserId, e.EventId });
            });

            //Emails
            modelBuilder.Entity<Emails>(entity =>
            {
                entity.HasKey(e => new { e.type, e.EventId });
            });

            //event images
            modelBuilder.Entity<EventImages>(entity =>
            {
                entity.HasKey(e => new { e.EventImage, e.EventId });
            });

            //invite 
            modelBuilder.Entity<Invite>(entity =>
            {
                entity.HasKey(e=>new { e.UserId, e.EventId });
            });

            //to do list
            modelBuilder.Entity<ToDoList>(entity =>
            {
                entity.HasKey(e => new { e.Title, e.EventId });
            });


            //userRole 
            modelBuilder.Entity<UserRole>(entity =>
            {
                entity.HasKey(e => new { e.UserId, e.RoleId });
            });


            base.OnModelCreating(modelBuilder); 
        }

    }
}