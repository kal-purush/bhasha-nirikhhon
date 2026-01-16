using System;
using System.Data.Entity;
using System.Linq;
using FinalProject_ADONET.Models;

namespace FinalProject_ADONET.EF
{
    public class DataManager : DbContext
    {        
        public DataManager()
            : base("name=DataManager")
        {}

        public virtual DbSet<Book> Books { get; set; }
        public virtual DbSet<Account> Accounts { get; set; }
        public virtual DbSet<Stock> Stocks { get; set; }
        public virtual DbSet<Genre> Genres { get; set; }
    }    
}