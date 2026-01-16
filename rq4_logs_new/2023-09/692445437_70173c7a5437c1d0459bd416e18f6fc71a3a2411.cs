using ISM.Application.interfaces;
using ISM.Domain.Models;
using ISM.Infrastructure.ISMDbcontext;

namespace ISM.Infrastructure.Repositories
{
    public class RepositoriyForOrders:IRepository<Order>
    {
         {private ISMdbcontext _dbcontext;

        private Validation.ValidationForOrders _validation;
        public RepositoriyForOrders()
        {
            _dbcontext = new ISMdbcontext();
            _validation = new Validation.ValidationForOrders();
        }
        public Order Create(Order Objectname)
        {
            if (_validation.Create(Objectname) == true)
            {
                _dbcontext.Orders.Add(Objectname);
                _dbcontext.SaveChanges();
                return Objectname;
            }
            return null;

        }
        public int Delete(int id)
        {
            if (_validation.Delete(id) == true)
            {
                Order? OrderDelete = _dbcontext.Orders.Find(id);
                _dbcontext.Orders.Remove(OrderDelete);
                _dbcontext.SaveChanges();
                return id;
            }
            return 0;
        }

        public IEnumerable<Order> GetAll()
        {
            if (_validation.Getall() == true)
                return _dbcontext.Orders.ToList();
            return null;

        }
        public Order Getbyid(int id)
        {
            if (_validation.Getby(id) == true)
                return _dbcontext.Orders.Find(id);
            return null;
        }
        public bool Update(Order objectname)
        {
            if (_validation.Update(objectname) == true)
            {
                _dbcontext.Orders.Update(objectname);
                _dbcontext.SaveChanges();
                return true;
            }
            return false;
        }
    }
}