
using BlogProject.Data;

namespace BlogProject.Generics;

public class EfRepository<T>(BlogContext ctx) : IRepository<T> where T : class
{
    private readonly BlogContext _ctx = ctx;

    public void Add(T entity)
    {
        _ctx.Set<T>().Add(entity);
    }

        public void Update(T entity)
    {
        _ctx.Set<T>().Update(entity);
    }

    public T? FindById(int id)
    {
        return _ctx.Find<T>(id);
    }

    public List<T?> ListAll()
    {
        return _ctx.Set<T>().ToList();
        // [.. _ctx.Set<T>()];
    }

    public void Remove(T entity)
    {
        _ctx.Remove(entity);
    }

    public void Save()
    {
        _ctx.SaveChanges();
    }
}