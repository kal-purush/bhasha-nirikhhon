using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Web;

namespace Internship.Models
{
    public class InternshipApplicationDbContext : ApplicationDbContext
    {
        DbSet<Internship> Internships { get; set; }
        

    }


    public class Internship
    {
        public bool Supervisor { get; set; }
        public bool Student { get; set; }
        public bool Dean { get; set; }
        public bool InternationalOffice { get; set; }
    }


}