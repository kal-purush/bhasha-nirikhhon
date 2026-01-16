using MagicVilla.Data;
using MagicVilla.Models;
using MagicVilla.Repository.IRepository;

namespace MagicVilla.Repository
{
    public class VillaRepository : Repository<Villa>, IVillaRepository
    {
        private readonly ApplicationDbContext _context;
        public VillaRepository(ApplicationDbContext context) 
            : base(context)
        {
            _context = context;
        }

        public void Update(Villa entity)
        {
            _context.Villas.Update(entity);
        }
    }
}