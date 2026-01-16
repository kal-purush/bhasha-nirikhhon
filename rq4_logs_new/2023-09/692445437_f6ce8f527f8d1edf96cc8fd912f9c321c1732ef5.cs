using ISM.Application.Interfaces.Orders;
using ISM.Domain.Models;
using ISM.Infrastructure.Repositories;

namespace ISM.Infrastructure.Services
{
    internal class ServiceForOrders
    {
        private IOrderRepository _repository;
        public ServiceForOrders() => _repository = new RepositoriyForOrders();
        public Order Create(Order Objectname)
        {
            _repository.Create(Objectname);
            return Objectname;
        }

        public int Delete(int id)
        {
            _repository.Delete(id);
            return id;
        }

        public IEnumerable<Order> GetAll()
        {
            return _repository.GetAll();
        }
        public Order Getbyid(int id)
        {
            return _repository.Getbyid(id);
        }

        public bool Update(Order objectname)
        {
            return _repository.Update(objectname);
        }
    }
}