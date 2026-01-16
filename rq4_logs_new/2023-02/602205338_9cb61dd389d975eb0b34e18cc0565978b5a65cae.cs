using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.EntityFrameworkCore;
using MTM_Holidays.Data;
using MTM_Holidays.Models;

namespace MTM_Holidays.Areas.Identity.Pages
{
    /// <summary>
    /// Page with all placed orders by user.
    /// 
    /// </summary>
    /// <author>Tomás Pinto</author>
    /// <version>24th Jan 2023</version>

    [Authorize]
	public class MyOrdersModel : PageModel
    {
        [BindProperty]
        public List<Order> Orders { get; set; }

        private readonly ApplicationDbContext _context;

        public MyOrdersModel(ApplicationDbContext context)
        {
            _context = context;
        }

        public async Task<IActionResult> OnGetAsync()
        {
            if (!User.Identity.IsAuthenticated)
            {
                return RedirectToPage("Unauthorized");
            }
            else
            {
                var orders = _context.Orders.Include(p => p.Customer)
                    .Include(p => p.DiscountCode)
                    .Include(p => p.Order_Holidays)
                    .ThenInclude(p => p.Holiday)
                    .Where(m => m.Customer.EmailAddress == User.Identity.Name)
                    .ToList();

                if (orders == null)
                {
                    return RedirectToPage("NotFound");
                }
                Orders = orders;
            }

            return Page();
        }

        public double CalculateTotal(Order order)
        {
            double total = 0;
            foreach (var order_holiday in order.Order_Holidays)
            {
                double cost;
                var holiday = order_holiday.Holiday;
                cost = holiday.Price * order_holiday.Night * order_holiday.Quantity;
                total += cost;
            }
            if (order.DiscountCode != null)
                total -= order.DiscountCode.Discount;
            return total;
        }
    }
}