using System;
using System.Threading.Tasks;
using Confab.Modules.Tickets.Core.Entities;
using Confab.Modules.Tickets.Core.Repositories;
using Microsoft.EntityFrameworkCore;

namespace Confab.Modules.Tickets.Core.DAL.Repositories
{
    internal class ConferenceRepository : IConferenceRepository
    {
        private readonly TicketsDbContext _context;
        private readonly DbSet<Conference> _conferences;

        public ConferenceRepository(TicketsDbContext context)
        {
            _context = context;
            _conferences = _context.Conferences;
        }

        public Task<Conference> GetAsync(Guid id)
        {
            throw new NotImplementedException();
        }

        public Task AddAsync(Conference conference)
        {
            throw new NotImplementedException();
        }

        public Task UpdateAsync(Conference conference)
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync(Conference conference)
        {
            throw new NotImplementedException();
        }
    }
}