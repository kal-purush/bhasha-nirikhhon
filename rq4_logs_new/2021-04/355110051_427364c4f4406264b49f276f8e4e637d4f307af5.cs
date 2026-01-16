using BlogBasic.Core.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BlogBasic.InfrastrutureCore.Context
{
    public class BlogContext : DbContext
    {

        public BlogContext(DbContextOptions<BlogContext> options) : base(options)
        {
            SeedSomeData();
        }

        private void SeedSomeData()
        {
            if (Posts.Count() == 0)
            {
                Posts.Add(new Post
                {
                    Id = 1000,
                    PostName = "My 1000 Post",
                    PostDescription = "Post for SDT genious students",
                    Content = "this is the new repository post in git",
                    TimeStamp = DateTime.UtcNow
                });
                Posts.Add(new Post
                {
                    Id = 1001,
                    PostName = "My 1001 Post",
                    PostDescription = "Post for SDT genious students",
                    Content = "this is the new repository post in git",
                    TimeStamp = DateTime.UtcNow
                });

                this.SaveChanges();
            }
        }

        public DbSet<Thread> Threads { get; set; }
        public DbSet<Post> Posts { get; set; }

    }
}