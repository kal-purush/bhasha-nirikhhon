namespace dks3Api.Models
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Web;
    using System.Data.Entity;
    using System.ComponentModel.DataAnnotations;
    using System.Data.Entity.Infrastructure;


    public class ApplicationDBContext : DbContext
    {
        public ApplicationDBContext()
            : base("name=DefaultConnection")
        {

        }

        public virtual DbSet<Weapon> Weapons { get; set; }
    }
}