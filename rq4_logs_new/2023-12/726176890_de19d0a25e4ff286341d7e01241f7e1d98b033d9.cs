using MagicVilla.Data;
using MagicVilla.Repository.IRepository;
using System.Runtime.CompilerServices;

namespace MagicVilla.Repository
{
    public class UnitOfWork : IUnitOfWork
    {
        private ApplicationDbContext _context;
        public UnitOfWork(ApplicationDbContext context)
        {
            _context = context;
            Villa = new VillaRepository(_context);
        }
        public IVillaRepository Villa { get; private set; }

        public async Task SaveAsync()
        {
            await _context.SaveChangesAsync();
        }
    }
}