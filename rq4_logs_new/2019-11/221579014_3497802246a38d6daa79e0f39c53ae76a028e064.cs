using ChajdPizzaWebApp.Data;
using ChajdPizzaWebApp.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ChajdPizzaWebApp.Repositories
{
    public class OrderDetailsRepo
    {
        private ApplicationDbContext _context;
        public OrderDetailsRepo(ApplicationDbContext ctx)
        {
            _context = ctx;
        }
        public async Task<OrderDetail> SelectById(int? id)
        {
            var orderDetail = await _context.OrderDetails.FirstOrDefaultAsync(o => o.Id == id);
            return orderDetail;
        }

        public async Task<List<OrderDetail>> SelectAll()
        {
            var orderDetails = await _context.OrderDetails.ToListAsync();
            return orderDetails;
        }
        public async Task<bool> Add(OrderDetail orderDetail)
        {

            _context.Add(orderDetail);
            await _context.SaveChangesAsync();
            return true;
        }
        public async Task<bool> Update(OrderDetail orderDetail)
        {
            _context.Update(orderDetail);
            await _context.SaveChangesAsync();
            return true;
        }
        public async Task<bool> Remove(OrderDetail orderDetail)
        {
            _context.Remove(orderDetail);
            await _context.SaveChangesAsync();
            return true;
        }
        public bool OrderExists(int id)
        {
            return _context.OrderDetails.Any(e => e.Id == id);
        }
    }
}