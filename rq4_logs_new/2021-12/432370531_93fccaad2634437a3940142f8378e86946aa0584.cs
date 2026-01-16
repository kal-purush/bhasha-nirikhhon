using DocBuilder.Core.ApiQuery;
using DocBuilder.Core.Extensions;
using DocBuilder.Data.Entities;
using DocBuilder.Data.Models;
using Microsoft.EntityFrameworkCore;

namespace DocBuilder.Data.Extensions
{
    public static class DocTExtensions
    {
        #region DocT

        static IQueryable<DocT> SetIncludes(this DbSet<DocT> docs) =>
            docs.Include(x => x.Category);

        static IQueryable<DocT> Search(this IQueryable<DocT> docs, string search) =>
            docs.Where(x =>
                x.Name.ToLower().Contains(search.ToLower())
                || (string.IsNullOrEmpty(x.Description) ? false : x.Description.ToLower().Contains(search.ToLower()))
                || (x.Category == null ? false : x.Category.Value.ToLower().Contains(search.ToLower()))
            );

        public static async Task<QueryResult<DocT>> QueryDocTs(
            this AppDbContext db,
            string page, string pageSize,
            string search, string sort
        )
        {
            var container = new QueryContainer<DocT>(
                db.DocTs
                    .SetIncludes(),
                page, pageSize, search, sort
            );

            return await container.Query((docs, s) =>
                docs.SetupSearch(s, (values, term) =>
                    values.Search(term)
                )
            );
        }

        public static async Task<DocT?> GetDocT(this AppDbContext db, int id) =>
            await db.DocTs
                .SetIncludes()
                .FirstOrDefaultAsync(x => x.Id == id);

        public static async Task<DocT> Clone(this DocT doc, AppDbContext db)
        {
            var clone = new DocT(doc.Name)
            {
                CategoryId = doc.CategoryId,
                Description = doc.Description,
                Items = await doc.CloneItems(db, CloneItems)
            };

            await db.DocTs.AddAsync(clone);
            await db.SaveChangesAsync();

            return clone;
        }

        public static async Task<Doc> Generate(this DocT doc, AppDbContext db)
        {
            var generated = new Doc(doc.Name)
            {
                CategoryId = doc.CategoryId,
                Description = doc.Description,
                Items = await doc.CloneItems(db, GenerateItems)
            };

            await db.Docs.AddAsync(generated);
            await db.SaveChangesAsync();

            return generated;
        }

        public static async Task<SaveResult> Save(this DocT doc, AppDbContext db)
        {
            var res = await doc.Validate(db);

            if (res.IsValid)
            {
                if (doc.Id > 0)
                    await doc.Update(db);
                else
                    await doc.Add(db);
            }

            return res;
        }

        public static async Task Remove(this DocT doc, AppDbContext db)
        {
            db.DocTs.Remove(doc);
            await db.SaveChangesAsync();
        }

        static async Task<ICollection<T>> CloneItems<T>(this DocT doc, AppDbContext db, Func<ICollection<DocItemT>, ICollection<T>> clone)
        {
            var items = await db.DocItemTs
                .Include(x => x.Options)
                .ToListAsync();

            var cloned = clone(items);

            return cloned;
        }

        static ICollection<DocItemT> CloneItems(ICollection<DocItemT> items)
        {
            var cloned = new List<DocItemT>();

            foreach (var item in items)
            {
                cloned.Add(new DocItemT(item.Value, item.Type)
                {
                    AllowMultiple = item.AllowMultiple,
                    Index = item.Index,
                    IsDropdown = item.IsDropdown,
                    Options = item.Options.Select(x => new DocOptionT(x.Value)).ToList()
                });
            }

            return cloned;
        }

        static ICollection<DocItem> GenerateItems(ICollection<DocItemT> items)
        {
            var generated = new List<DocItem>();

            foreach (var item in items)
            {
                generated.Add(new DocItem(item.Value, item.Type)
                {
                    AllowMultiple = item.AllowMultiple,
                    Index = item.Index,
                    IsDropdown = item.IsDropdown,
                    Options = item.Options.Select(x => new DocOption(x.Value)).ToList()
                });
            }

            return generated;
        }

        static async Task Add(this DocT doc, AppDbContext db)
        {
            await db.DocTs.AddAsync(doc);
            await db.SaveChangesAsync();
        }

        static async Task Update(this DocT doc, AppDbContext db)
        {
            db.DocTs.Update(doc);
            await db.SaveChangesAsync();
        }

        static async Task<SaveResult> Validate(this DocT doc, AppDbContext db)
        {
            if (string.IsNullOrEmpty(doc.Name))
                return new SaveResult { IsValid = false, Message = "Document Template must have a name." };

            var check = await db.Docs
                .FirstOrDefaultAsync(x =>
                    x.Id != doc.Id
                    && x.Name.ToLower() == doc.Name.ToLower()
                );

            return check is null
                ? new SaveResult { IsValid = true }
                : new SaveResult { IsValid = false, Message = $"Document Template name {doc.Name} is already in use." };
        }

        #endregion

        #region ItemT

        public static async Task<List<DocItemT>> GetDocItemTs(this AppDbContext db, int docTId) =>
            await db.DocItemTs
                .Where(x => x.DocTId == docTId)
                .OrderBy(x => x.Index)
                .ToListAsync();

        public static async Task<DocItemT?> GetDocItemT(this AppDbContext db, int id) =>
            await db.DocItemTs
                .FindAsync(id);

        public static async Task<SaveResult> Save(this DocItemT item, AppDbContext db)
        {
            var res = item.Validate();

            if (res.IsValid)
            {
                if (item.Id > 0)
                    await item.Update(db);
                else
                    await item.Add(db);
            }

            return res;
        }

        public static async Task Remove(this DocItemT item, AppDbContext db)
        {
            db.DocItemTs.Remove(item);
            await db.SaveChangesAsync();
        }

        static async Task Add(this DocItemT item, AppDbContext db)
        {
            await db.DocItemTs.AddAsync(item);
            await db.SaveChangesAsync();
        }

        static async Task Update(this DocItemT item, AppDbContext db)
        {
            db.DocItemTs.Update(item);
            await db.SaveChangesAsync();
        }

        static SaveResult Validate(this DocItemT item)
        {
            if (item.DocTId < 1)
                return new SaveResult { IsValid = false, Message = "An Item Template must be associated with a Document Template." };

            if (string.IsNullOrEmpty(item.Value))
                return new SaveResult { IsValid = false, Message = "An Item Template must have a value." };

            if (item.Type == DocItemType.Select && item.Id < 1)
            {
                if (item.Options.Count < 1)
                    return new SaveResult { IsValid = false, Message = "A new Select Item Template must contain at least one Option Template." };

                var res = item.Options.Validate();

                if (!res.IsValid) return res;
            }

            return new SaveResult { IsValid = true };
        }

        #endregion

        #region OptionT

        public static async Task<List<DocOptionT>> GetDocOptionTs(this AppDbContext db, int selectId) =>
            await db.DocOptionTs
                .Where(x => x.DocItemTId == selectId)
                .OrderBy(x => x.Value)
                .ToListAsync();

        public static async Task<DocOptionT?> GetDocOptionT(this AppDbContext db, int id) =>
            await db.DocOptionTs
                .FindAsync(id);

        public static async Task<SaveResult> Save(this DocOptionT option, AppDbContext db)
        {
            var res = await option.Validate(db);

            if (res.IsValid)
            {
                if (option.Id > 0)
                    await option.Update(db);
                else
                    await option.Add(db);
            }

            return res;
        }

        public static async Task Remove(this DocOptionT option, AppDbContext db)
        {
            db.DocOptionTs.Remove(option);
            await db.SaveChangesAsync();
        }

        static async Task Add(this DocOptionT option, AppDbContext db)
        {
            await db.DocOptionTs.AddAsync(option);
            await db.SaveChangesAsync();
        }

        static async Task Update(this DocOptionT option, AppDbContext db)
        {
            db.DocOptionTs.Update(option);
            await db.SaveChangesAsync();
        }

        static async Task<SaveResult> Validate(this DocOptionT option, AppDbContext db)
        {
            if (string.IsNullOrEmpty(option.Value))
                return new SaveResult { IsValid = false, Message = "An Option Template must have a value." };

            if (option.DocItemTId < 1)
                return new SaveResult { IsValid = false, Message = "An Option Template must be associated with a Select Item Template." };

            var check = await db.DocOptionTs
                .FirstOrDefaultAsync(x =>
                    x.Id != option.Id
                    && x.Value.ToLower() == option.Value.ToLower()
                );

            return check is null
                ? new SaveResult { IsValid = true }
                : new SaveResult { IsValid = false, Message = $"Option ${option.Value} is already used in this Select Item Template." };
        }

        static SaveResult Validate(this DocOptionT option)
        {
            if (string.IsNullOrEmpty(option.Value))
                return new SaveResult { IsValid = false, Message = "An Option Template must have a value." };

            return new SaveResult { IsValid = true };
        }

        static SaveResult Validate(this ICollection<DocOptionT> options)
        {
            foreach (var option in options)
            {
                var res = option.Validate();
                if (!res.IsValid) return res;

                if (options.Count(x => x.Value.ToLower() == option.Value.ToLower()) > 1)
                    return new SaveResult
                    {
                        IsValid = false,
                        Message = "Values in a Select Item Options List Template must be distinct."
                    };
            }

            return new SaveResult { IsValid = true };
        }

        #endregion
    }
}