using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.EntityFrameworkCore;
using YanSoft.CurrencyExchanger.Core.Models;

namespace YanSoft.CurrencyExchanger.Core.Data
{
    public class CurrencyDataContext : DbContext
    {
        public DbSet<CurrencyExchangeItem> CurrencyExchangeItems { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlite("Data Source=sqlite.db");
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CurrencyExchangeItem>(
                c =>
                {
                    c.Ignore(x => x.SourceCurrency);
                    c.Ignore(x => x.TargetCurrency);
                    c.HasKey(x => x.Id);
                });
        }


    }
}