using MB.Application.Interfaces.Persistence;
using MB.Domain.Entities;
using MB.Persistence.Repositories.Common;
using Microsoft.EntityFrameworkCore;

namespace MB.Persistence.Repositories;

public class TagCategoryRepository(Context context) : BaseRepository<TagCategory>(context), ITagCategoryRepository
{
    public async System.Threading.Tasks.Task DeleteTagCategoryTags(int tagCategoryId)
    {
        var tags = await _context.Tags
                                .Where(tag => tag.TagCategoryId == tagCategoryId)
                                .ToListAsync();

        _context.Tags.RemoveRange(tags);

        await _context.SaveChangesAsync();
    }
}