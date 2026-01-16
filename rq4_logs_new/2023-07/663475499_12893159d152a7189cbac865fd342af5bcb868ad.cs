
using Demo.Models;
using Microsoft.EntityFrameworkCore;



namespace Demo.Context
{
    public class ApplicationDBContext :DbContext
    {

        public ApplicationDBContext(DbContextOptions<ApplicationDBContext> options):base(options)
        { 
        
        }

        public virtual DbSet<Users> Users { get; set; }
        public virtual DbSet<Student> Student { get; set; }
      
    }
}