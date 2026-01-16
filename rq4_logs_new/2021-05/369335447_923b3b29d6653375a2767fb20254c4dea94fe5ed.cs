using System;
namespace ACM.BL
{
    public class Order
    {
        public Order()
        {
        }
        public Order(int orderId)
        {
            OrderId = orderId;
        }

        public int OrderId { get; private set; }
        public DateTimeOffset? OrderDate { get; set; }


        public bool Validate()
        {
            if (OrderDate == null)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
    }
}