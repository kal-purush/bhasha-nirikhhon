using ChajdPizzaWebApp.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ChajdPizzaWebApp.Repositories.Interfaces
{
    public interface IOrderDetailsRepo
    {
        public Task<OrderDetail> SelectById(int? id);

        public Task<List<OrderDetail>> SelectAll();

        public Task<List<OrderDetail>> SelectOrderAllDetails(int? orderId);

        public Task<bool> Add(OrderDetail orderDetail);

        public Task<bool> Update(OrderDetail orderDetail);

        public Task<bool> Remove(OrderDetail orderDetail);

        public bool OrderDetailExists(int id);
    }
}