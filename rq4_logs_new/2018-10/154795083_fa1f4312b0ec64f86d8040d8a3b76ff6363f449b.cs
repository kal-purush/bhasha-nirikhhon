using System.Data.Entity;
using System.Threading.Tasks;
using TheShop.DataAccess;
using TheShop.Model;

namespace TheShop.MVVM.Data.Repositories
{
	public class ProductRepository : IProductRepository
	{
		private ProductDbContext _context;

		public ProductRepository(ProductDbContext context)
		{
			_context = context;
		}

		public async Task<Product> GetByIdAsync(int productId)
		{
			//Load data from database
			return await _context.Products.SingleAsync(p => p.Id == productId);
		}

		public async Task SaveASync()
		{
			await _context.SaveChangesAsync();
		}
	}
}