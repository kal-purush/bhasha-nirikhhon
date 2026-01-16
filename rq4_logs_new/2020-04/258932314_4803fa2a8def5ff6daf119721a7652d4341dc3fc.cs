using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using SPS.AEM.Database.Contexts;
using SPS.AEM.Repository.Interfaces;
using SPS.AEM.Repository.Models;

namespace SPS.AEM.Repository.Implementations
{
    public class DistrictRepository : IDistrictRepository
    {
        private readonly AemDatabaseContext context;

        public DistrictRepository(AemDatabaseContext context)
        {
            this.context = context;
        }

        public async Task<IEnumerable<DistrictDto>> GetAllDistrictsAsync()
        {
            return await this.context.Districts
                .Select(p => new DistrictDto { Id = p.Id, Name = p.Name })
                .ToListAsync();
        }
    }
}