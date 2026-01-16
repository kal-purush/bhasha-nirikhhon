using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFTest
{
    class AppContext : DbContext
    {
        public AppContext()
            : base("EFTestDatabase")  // Name should match the connection string name since it's different than the context name
        {

        }

        public DbSet<Presentation> Presentations { get; set; }
    }
}