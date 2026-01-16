using ChajdPizzaWebApp.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ChajdPizzaWebApp.Repositories.Interfaces
{
    public interface IOrdersRepo
    {
        public Task<Orders> SelectById(int? id);

        public Task<Orders> SelectByCustId(int? id);

        public Task<Orders> SelectMultByCustId(int? id, int Oid);

        public Task<List<Orders>> SelectAll();

        public Task<bool> Add(Orders order);

        public Task<bool> Update(Orders order);

        public Task<bool> Remove(Orders order);

        public bool OrderExists(int id);
    }
}