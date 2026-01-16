using Microsoft.EntityFrameworkCore;
using PetAdoptSite.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PetAdoptSite.Data
{
    /// <summary>
    /// DB context class for pets
    /// </summary>
    public class PetContext : DbContext
    {
        public PetContext(DbContextOptions<PetContext> options)
            : base(options)
        {
            
        }

        // Add a DB for each enity you want to add to
        // keep track of in the database
        public DbSet<Pets> Pets { get; set; }
    }
}