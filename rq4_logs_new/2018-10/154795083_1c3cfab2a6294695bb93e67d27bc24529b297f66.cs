using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using TheShop.DataAccess;
using TheShop.Model;

namespace TheShop.Data
{
	public class LookupDataService : IProductLookupDataService
	{
		private Func<ProductDbContext> _contextCreator;

		public LookupDataService(Func<ProductDbContext> contextCreator)
		{
			_contextCreator = contextCreator;
		}

		public async Task<IEnumerable<LookupItem>> GetProductLookupAsync()
		{
			using(var ctx = _contextCreator())
			{
				return await ctx.Products.AsNoTracking()
					.Select(p =>
					new LookupItem
					{
						Id = p.Id,
						DisplayProduct = p.Name
					})
					.ToListAsync();
			}
		}
	}
}