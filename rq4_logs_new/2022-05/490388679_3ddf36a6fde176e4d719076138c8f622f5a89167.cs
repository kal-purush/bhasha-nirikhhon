using Blog.Data;
using Blog.Dtos.CategoryDtos;
using Blog.Models;
using Blog.Repositories.Interfaces;
using Ether.Outcomes;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;

namespace Blog.Repositories;

public class CategoryRepository : ICategoryRepository
{
    private readonly IMemoryCache _cache;

    private readonly BlogDataContext _context;

    public CategoryRepository(BlogDataContext context, IMemoryCache cache)
    {
        _context = context;
        _cache = cache;
    }

    public async Task<IOutcome<Category>> CreateAsync(EditorCategoryDto categoryRequest)
    {
        var category = new Category
        {
            Name = categoryRequest.Name,
            Slug = categoryRequest.Slug
        };

        await _context.Categories.AddAsync(category);
        await _context.SaveChangesAsync();

        return Outcomes
            .Success<Category>()
            .WithValue(category);
    }

    public async Task<IOutcome<IEnumerable<Category>>> GetAllAsync(int page, int pageSize)
    {
        var categoriesFromCache = _cache.Get<IEnumerable<Category>>("CategoriesCache");

        if(categoriesFromCache is not null)
            return Outcomes
                .Success<IEnumerable<Category>>()
                .WithValue(categoriesFromCache);

        var catagoriesFromDb = await GetCategoriesAsync(_context, page, pageSize);

        if(catagoriesFromDb.Count() is 0)
            return Outcomes
                .Failure<IEnumerable<Category>>()
                .WithMessage("No categories found.");

        _cache.Set(
            "CategoriesCache",
            catagoriesFromDb,
            new MemoryCacheEntryOptions
            {
                AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1)
            });

        return Outcomes
            .Success<IEnumerable<Category>>()
            .WithValue(catagoriesFromDb);
    }

    public async Task<IOutcome<Category>> GetByIdAsync(int id)
    {
        var categoryFromCache = _cache.Get<Category>("CategoryCache");

        if(categoryFromCache is not null)
            return Outcomes
                .Success<Category>()
                .WithValue(categoryFromCache);

        var categoryFromDb = await _context
            .Categories
            .AsNoTracking()
            .FirstOrDefaultAsync(cat => cat.Id == id);

        if(categoryFromDb is null)
            return Outcomes.Failure<Category>();

        _cache.Set(
            "CategoryCache",
            categoryFromDb,
            new MemoryCacheEntryOptions
            {
                AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1)
            });

        return Outcomes
            .Success<Category>()
            .WithValue(categoryFromDb);
        
    }

    public async Task<IOutcome<Category>> UpdateAsync(int id, EditorCategoryDto categoryRequest)
    {
        var doesCategoryExists = await _context
            .Categories
            .AsNoTracking()
            .AnyAsync(cat => cat.Id == id);

        if(!doesCategoryExists)
            return Outcomes.Failure<Category>();

        var category = new Category 
        {
            Name = categoryRequest.Name,
            Slug = categoryRequest.Slug
        };

        _context.Categories.Update(category);
        await _context.SaveChangesAsync();
        
        return Outcomes
            .Success<Category>()
            .WithValue(category);
    }

    public async Task<IOutcome> RemoveAsync(int id)
    {
        var category = await _context
            .Categories
            .AsNoTracking()
            .FirstOrDefaultAsync(cat => cat.Id == id);

        if(category is null)
            return Outcomes.Failure();

        _context.Remove(category);
        await _context.SaveChangesAsync();

        return Outcomes.Success();
    }

    private async Task<List<Category>> GetCategoriesAsync(
        BlogDataContext context,
        int page,
        int pageSize)
            => await context.Categories
            .Skip(page * pageSize)
            .Take(pageSize)
            .AsNoTracking()
            .ToListAsync(); 
}