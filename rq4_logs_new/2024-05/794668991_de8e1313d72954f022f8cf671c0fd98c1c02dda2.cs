using Microsoft.EntityFrameworkCore;
using TaskManagementSystem.Entity;

namespace TaskManagementSystem.Data
{
    public class TaskManagementDBContext : DbContext
    {
        public TaskManagementDBContext(DbContextOptions<TaskManagementDBContext> options) : 
            base(options)
        {
                
        }
        public DbSet<Entity.Task> Tasks { get; set; }
        public DbSet<User> Users { get; set; }
        public DbSet<Department> Departments { get; set; }
        public DbSet<Status> Statuses { get; set; }
        public DbSet<Attachment> Attachments { get; set; }
        public DbSet<Comment> Comments { get; set; }

        //protected override void OnModelCreating(DbModelBuilder modelBuilder)
        //{

        //    // Task to Status relationship
        //    modelBuilder.Entity<Entity.Task>()
        //        .HasRequired(t => t.Status)
        //        .WithMany(s => s.Tasks)
        //        .HasForeignKey(t => t.StatusId)
        //        .WillCascadeOnDelete(false);

        //    // User to Department relationship
        //    modelBuilder.Entity<User>()
        //        .HasRequired(u => u.Department)
        //        .WithMany(d => d.Users)
        //        .HasForeignKey(u => u.DepartmentId)
        //        .WillCascadeOnDelete(false);

        //    // Task to User (TeamMembers) relationship
        //    modelBuilder.Entity<Entity.Task>()
        //        .HasMany(t => t.TeamMembers)
        //        .WithMany(u => u.Tasks)
        //        .Map(m =>
        //        {
        //            m.ToTable("TaskUser");
        //            m.MapLeftKey("TaskId");
        //            m.MapRightKey("UserId");
        //        });

        //    // Task to Attachment relationship
        //    modelBuilder.Entity<Entity.Task>()
        //        .HasMany(t => t.Attachments)
        //        .WithRequired(a => a.Task)
        //        .HasForeignKey(a => a.TaskId)
        //        .WillCascadeOnDelete(false);

        //    // Task to Comment relationship
        //    modelBuilder.Entity<Entity.Task>()
        //        .HasMany(t => t.Comments)
        //        .WithRequired(c => c.Task)
        //        .HasForeignKey(c => c.TaskId)
        //        .WillCascadeOnDelete(false);
        //}

    }
}