using PocketForzaHorizonCommunity.Back.Database.Entities.Guides;
using PocketForzaHorizonCommunity.Back.Database.Repos.Interfaces;

namespace PocketForzaHorizonCommunity.Back.Database.Repos;

public class GalleryRepository : IGalleryRepository
{
    private ApplicationDbContext _context;
    public GalleryRepository(ApplicationDbContext context)
    {
        _context = context;
    }

    public IQueryable<GalleryImage> GetByDesignId(Guid id) => _context.Set<GalleryImage>().Where(g => g.DesignOptionsId == id).AsQueryable();

    public async Task CreateAsync(GalleryImage entity) => await _context.Set<GalleryImage>().AddAsync(entity);

    public void Delete(GalleryImage entity) => _context.Set<GalleryImage>().Remove(entity);

    public async Task SaveAsync() => await _context.SaveChangesAsync();
}