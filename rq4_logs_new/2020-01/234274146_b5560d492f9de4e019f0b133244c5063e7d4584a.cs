using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Web;

namespace WebApplication1.Models
{
    //数据库上下文
    public class MYDBContext : DbContext
    {
        public MYDBContext()
            : base("name=conncodefirst")
        {
        }

        public DbSet<UserTs> Customer { get; set; }

        public DbSet<code_value> code_values { get; set; }
    }
}