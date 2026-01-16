using BookWebEcommerce.Models;
using Microsoft.EntityFrameworkCore;

namespace BookWebEcommerce.Data.Services
{
	public class OrdersService : IOrdersServices
	{
		private readonly AppDbContext _context;

		public OrdersService(AppDbContext context)
		{
			_context = context;
		}
		public async Task<List<Order>> GetAllOrdersAsync(string userId)
		{
			var items =await _context.Orders.Include(n => n.OrderItems).ThenInclude(b => b.Book)
				.Where(n => n.UserId == userId).ToListAsync();
			return items;
		}

		public async Task StoreOrderAsync(List<ShoppingCartItem> items, string userId, string userEmailAddress)
		{
			var order = new Order()
			{
				UserId = userId,
				Email = userEmailAddress
			};
			await _context.Orders.AddAsync(order);
			await _context.SaveChangesAsync();

			foreach (var item in items)
			{
				var orderItem = new OrderItem()
				{
					Amount = item.Amount,
					BookId = item.Book.Id,
					OrderId = order.Id,
					Price = item.Book.Price
				};
				await _context.OrderItems.AddAsync(orderItem);
			}
			await _context.SaveChangesAsync();

		}
	}
}