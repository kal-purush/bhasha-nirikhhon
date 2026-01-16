using AutoBogus;
using Bogus;
using Distillery.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UniTests.Shared
{
    public static class AppEntityFaker
    {
        public static AutoFaker<DbSet<CreditCard>> GetDbSetCreditCard()
        {
            return new AutoFaker<DbSet<CreditCard>>();
        }


    }
}