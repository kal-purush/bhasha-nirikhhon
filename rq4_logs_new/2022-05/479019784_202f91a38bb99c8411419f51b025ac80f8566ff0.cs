using Microsoft.EntityFrameworkCore;
using ResidentSleeper.Contexts;
using ResidentSleeper.Interfaces;
using ResidentSleeper.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ResidentSleeper.Services
{
    public class OrderService : IOrderService
    {
        private readonly MainContext _context;

        public OrderService(MainContext context)
        {
            _context = context;
        }

        public async Task Create(OrderWithDetails order)
        {
            try
            {
                _context.Orders.Add(order.Order);
                _context.OrderDetails.AddRange(order.Details);
                await _context.SaveChangesAsync();
            }
            catch (Exception)
            {
                //error
            }
        }

        public async Task Delete(int id)
        {
            try
            {
                var order = _context.Orders.FirstOrDefault(x => x.Id == id);
                if (order == null) return;

                var details = await _context.OrderDetails.Where(x => x.orderId == order.Id).ToListAsync();
                _context.OrderDetails.RemoveRange(details);
                _context.Orders.Remove(order);

                await _context.SaveChangesAsync();
            }
            catch(Exception)
            {
                //error
            }
        }

        public async Task<List<OrderWithDetails>> GetAll()
        {
            var ordersWithDetailsList = new List<OrderWithDetails>();
            try
            {
                var orders = await _context.Orders.ToListAsync();
                var details = await _context.OrderDetails.ToListAsync();

                orders.ForEach(o => ordersWithDetailsList.Add(new OrderWithDetails()
                {
                    Order = o,
                    Details = details.Where(d => d.orderId == o.Id).ToList()
                }));
            }
            catch(Exception)
            {
                //error
            }
            return ordersWithDetailsList;
        }

        public async Task<OrderWithDetails> GetById(int id)
        {
            var order = _context.Orders.FirstOrDefault(x => x.Id == id);
            if (order == null) return new OrderWithDetails();

            var details = await _context.OrderDetails.Where(x => x.orderId == order.Id).ToListAsync();


            return new OrderWithDetails()
            {
                Order = order,
                Details = details
            };
        }

        public async Task<List<OrderWithDetails>> GetByUserId(int id)
        {
            var ordersWithDetailsList = new List<OrderWithDetails>();
            try
            {
                var orders = await _context.Orders.Where(o => o.UserId == id).ToListAsync();
                var details = await _context.OrderDetails.ToListAsync();

                orders.ForEach(o => ordersWithDetailsList.Add(new OrderWithDetails()
                {
                    Order = o,
                    Details = details.Where(d => d.orderId == o.Id).ToList()
                }));
            }
            catch (Exception)
            {
                //error
            }
            return ordersWithDetailsList;
        }

        public async Task Update(int id, OrderWithDetails orderWithDetails)
        {
            try
            {
                var order = orderWithDetails.Order;
                var details = orderWithDetails.Details;

                var oldOrder = _context.Orders.FirstOrDefault(o => o.Id == order.Id);
                if (oldOrder == null) return;

                var oldDetailsList = await _context.OrderDetails.Where(d => d.orderId == order.Id).ToListAsync();
                oldOrder.Id = order.Id;
                oldOrder.UserId = order.UserId;
                oldOrder.Status = order.Status;
                oldOrder.Cost = order.Cost;

                foreach (var det in details)
                {
                    if (!oldDetailsList.Contains(det))
                    {
                        oldDetailsList.Add(det);
                    }
                }
                await _context.SaveChangesAsync();
            }
            catch (Exception)
            {
                //error
            }
        }
    }
}