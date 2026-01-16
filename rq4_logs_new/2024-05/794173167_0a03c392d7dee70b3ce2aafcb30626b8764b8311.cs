using Microsoft.EntityFrameworkCore;
using WebGeoInfrastructure.Entities;
using WebGeoInfrastructure.Interfaces.Repositories;

namespace WebGeoRepository.Repositories
{
    public class ClientRepository : IClientRepository
    {
        private readonly AppDBContext _context;
        public ClientRepository(AppDBContext context)
        {
            _context = context;
        }
        public Task<Client> Edit(Client client)
        {
            throw new NotImplementedException();
        }

        public async Task<List<Client>> GetAll()
        {
            return await _context.Clients.ToListAsync();
        }

        public async Task<Client?> GetById(int id)
        {
            return await _context.Clients.Where(c => c.Id == id).FirstOrDefaultAsync();
        }

        public async Task<bool> Insert(Client client)
        {
            try
            {
                _context.Clients.Add(client);
                _context.SaveChanges();
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }
    }
}