using DAL.Entities;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

namespace DAL
{
    public class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
        {

        }

        public DbSet<User> Users { get; set; }
        public DbSet<Post> Posts { get; set; }
        public DbSet<PostReply> PostReplies { get; set; }
        public DbSet<Like> Likes { get; set; }
        public DbSet<Dislike> Dislikes { get; set; }
        public DbSet<Topic> Topics { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<PostReply>(b =>
            {
                b.HasKey(pr => pr.Id);
                b.Property(pr => pr.Content).HasMaxLength(3000);

                b.HasOne(e => e.Post)
                    .WithMany(e => e.PostReplies)
                    .HasForeignKey(p => p.PostId)
                    .IsRequired()
                    .OnDelete(DeleteBehavior.ClientCascade);

                b.HasOne(e => e.Author)
                    .WithMany(e => e.PostReplies)
                    .HasForeignKey(p => p.UserId)
                    .IsRequired()
                    .OnDelete(DeleteBehavior.ClientCascade);

                b.HasOne(e => e.ParentReply)
                    .WithMany(e => e.Replies)
                    .HasForeignKey(p => p.ParentReplyId)
                    .OnDelete(DeleteBehavior.ClientCascade);
            });

            modelBuilder.Entity<Post>(b =>
            {
                b.HasKey(p => p.Id);

                b.Property(p => p.Description).HasMaxLength(50);
                b.Property(p => p.Content).HasMaxLength(3000);

                b.HasOne(e => e.Author)
                    .WithMany(e => e.Posts)
                    .HasForeignKey(p => p.UserId)
                    .IsRequired()
                    .OnDelete(DeleteBehavior.ClientCascade);

                b.HasOne(e => e.Topic)
                    .WithMany(e => e.Posts)
                    .HasForeignKey(p => p.TopicId)
                    .OnDelete(DeleteBehavior.SetNull);
            });

            modelBuilder.Entity<User>(b =>
            {
                b.HasKey(u => u.Id);

                b.Property(u => u.UserName).HasMaxLength(30).IsRequired();
                //b.Property(u => u.HashedPassword).IsRequired();
                //b.Property(u => u.Email).IsRequired();
            });

            modelBuilder.Entity<Like>(b =>
            {
                b.HasKey(l => new {l.UserId, l.PostId});

                b.HasOne(e => e.User)
                    .WithMany(e => e.Likes)
                    .HasForeignKey(p => p.UserId)
                    .IsRequired()
                    .OnDelete(DeleteBehavior.ClientCascade);

                b.HasOne(e => e.Post)
                    .WithMany(e => e.Likes)
                    .HasForeignKey(p => p.PostId)
                    .IsRequired()
                    .OnDelete(DeleteBehavior.ClientCascade);

                b.HasOne(e => e.PostReply)
                    .WithMany(e => e.Likes)
                    .HasForeignKey(p => p.PostReplyId)
                    .OnDelete(DeleteBehavior.ClientCascade);
            });

            modelBuilder.Entity<Dislike>(b =>
            {
                b.HasKey(d => new { d.UserId, d.PostId });

                b.HasOne(e => e.User)
                    .WithMany(e => e.Dislikes)
                    .HasForeignKey(p => p.UserId)
                    .IsRequired()
                    .OnDelete(DeleteBehavior.ClientCascade);

                b.HasOne(e => e.Post)
                    .WithMany(e => e.Dislikes)
                    .HasForeignKey(p => p.PostId)
                    .IsRequired()
                    .OnDelete(DeleteBehavior.ClientCascade);

                b.HasOne(e => e.PostReply)
                    .WithMany(e => e.Dislikes)
                    .HasForeignKey(p => p.PostReplyId)
                    .OnDelete(DeleteBehavior.ClientCascade);
            });

            modelBuilder.Entity<Topic>(b =>
            {
                b.HasKey(t => t.Id);
                b.Property(t => t.Name).HasMaxLength(100);
            });
        }
    }
}