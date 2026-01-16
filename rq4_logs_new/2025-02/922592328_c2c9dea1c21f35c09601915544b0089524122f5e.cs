using Microsoft.EntityFrameworkCore;
using Monitoring.Repositories;

public class Repository<T> : IRepository<T> where T : class
{
    private readonly MonitoringDbContext _context;
    private readonly DbSet<T> _entities;

    public Repository(MonitoringDbContext context)
    {
        _context = context;
        _entities = context.Set<T>();
    }

    public async Task AddAsync(T entity) => await _entities.AddAsync(entity);

    public async Task<T?> GetByIdAsync(int id) => await _entities.FindAsync(id);

    public async Task<IEnumerable<T>> GetAllAsync() => await _entities.ToListAsync();

    public void Update(T entity) => _entities.Update(entity);

    public void Delete(T entity) => _entities.Remove(entity);

    public async Task SaveAsync() => await _context.SaveChangesAsync();
    
    public void Delete(int id)
    {
        var entity = _entities.Find(id);
        if (entity != null)
        {
            _entities.Remove(entity);
        }
    }
}