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
    public class FeederRepository : IFeederRepository
    {
        private readonly AemDatabaseContext context;

        public FeederRepository(AemDatabaseContext context)
        {
            this.context = context;
        }

        public async Task<IEnumerable<FeederDto>> GetFeeders(int sectionId)
        {
            return await context.Feeders.Where(p => p.SectionId == sectionId)
                .Select(p => new FeederDto{ Id = p.Id, Name = p.Name, SectionId = p.SectionId})
                .ToListAsync();
        }

        public async Task AddFeeder(FeederDto feeder)
        {
            context.Feeders.Add(new Feeder{ Name = feeder.Name, SectionId = feeder.SectionId });
            await context.SaveChangesAsync();
        }
    }
}