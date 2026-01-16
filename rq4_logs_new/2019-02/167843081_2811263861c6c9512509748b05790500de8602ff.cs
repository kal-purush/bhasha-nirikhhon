using _FinalProject.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace _FinalProject.Context
{
    public class FinalProjectDBContext: DbContext
    {
        public DbSet<User>Users { get; set; }
        public DbSet<Robin>Robins { get; set; }
        public DbSet<UsersByRobin> UsersByRobins { get; set; }
        public DbSet<Letter>Letters { get; set; }
        public DbSet<Post> Posts { get; set; }
        public DbSet<Comment>Comments { get; set; }
        public DbSet<SubmissionStatus> SubmissionStatuses { get; set; }
        public DbSet <Calendar>Calendars { get; set; }
        public DbSet<Map>Maps { get; set; }

        public FinalProjectDBContext(DbContextOptions<FinalProjectDBContext> options) : base(options)
        {

        }

    }
}
