using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WpfApp1.Models;
using Microsoft.EntityFrameworkCore;

namespace WpfApp1
{
    public class ApplicationContext : DbContext
    {
 /*       private string path = @"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename=C:\Users\vdtri\source\repos\Wpf_C#_2 — копия\WpfApp1\Library.mdf;Integrated Security=True";*/
        public DbSet<Image_db> Images { get; set; }
        public DbSet<Detail> Details { get; set; }
        protected override void OnConfiguring(DbContextOptionsBuilder o)
            => o.UseSqlite(@"Data source = C:\Users\vdtri\source\repos\Wpf_C#_2\WpfApp1\Library.db");
    }
}