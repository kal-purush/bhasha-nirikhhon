using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DisneyCafe.Models.Database;
using Microsoft.EntityFrameworkCore;

namespace DisneyCafe.Data
{
    public static class DessertsDb
    {
        public static async Task<List<Desserts>> GetDessertsAsync(ApplicationDbContext context)
        {
            List<Desserts> desserts =
                await (from d in context.Desserts
                       select d).ToListAsync();
            return desserts;
        }
    }
}