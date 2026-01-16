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
    public class LoadTypeRepository : ILoadTypeRepository
    {
        private readonly AemDatabaseContext context;

        public LoadTypeRepository(AemDatabaseContext context)
        {
            this.context = context;
        }

        public async Task<IEnumerable<LoadTypeDto>> GetAllLoadTypesAsync()
        {
            return await this.context.LoadTypes
                .Select(p => new LoadTypeDto { Id = p.Id, Name = p.Type })
                .ToListAsync();
        }
    }
}