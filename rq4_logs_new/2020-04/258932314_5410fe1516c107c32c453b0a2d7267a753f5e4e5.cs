using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using SPS.AEM.Database.Contexts;
using SPS.AEM.Database.Entities;
using SPS.AEM.Repository.Interfaces;
using SPS.AEM.Repository.Models;

namespace SPS.AEM.Repository.Implementations
{
    public class SectionRepository : ISectionRepository
    {
        private readonly AemDatabaseContext context;

        public SectionRepository(AemDatabaseContext context)
        {
            this.context = context;
        }

        public async Task<IEnumerable<SectionDto>> GetSectionsAsync(int stationId)
        {
            return await context.Sections.Where(p => p.StationId == stationId)
                .Select(p => new SectionDto{ Id = p.Id, Name = p.Name, StationId = p.StationId})
                .ToListAsync();
        }

        public async Task AddSectionAsync(SectionDto section)
        {
            context.Sections.Add(new Section{ Name = section.Name, StationId = section.StationId });
            await context.SaveChangesAsync();
        }
    }
}