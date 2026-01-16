using System;
namespace ACM.BL.Repositories
{
    public class OrderRepository
    {
        public Order Retrieve(int orderId)
        {
            Order order = new Order(orderId);

            if (orderId == 10)
            {
                order.OrderDate = new DateTimeOffset(DateTime.Now);
            }

            return order;
        }

        public bool Save(Order order)
        {
            if (order != null)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}