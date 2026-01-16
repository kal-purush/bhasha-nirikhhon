using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TestWebApp.Models;

namespace TestWebApp.Contexts
{
    public class HeroesContext:DbContext
    {
        public DbSet<HeroDbModel> Heroes { get; set; }
        public DbSet<OwnerDbModel> Owners { get; set; }

        public HeroesContext(DbContextOptions opt) : base(opt)
        {

        }


    }
}