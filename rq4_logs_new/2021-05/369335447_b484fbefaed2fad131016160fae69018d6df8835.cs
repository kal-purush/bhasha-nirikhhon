using System;
namespace ACM.BL
{
    public class OrderItem
    {
        public OrderItem()
        {
        }

        public OrderItem(int orderId)
        {
            OrderId = orderId;
        }

        public int OrderId { get; private set; }
        public decimal? PurchasePrice { get; set; }
        public int ProductId { get; set; }
        public int Quantity { get; set; }

        public bool Validate()
        {
            if (Quantity <= 0)
            {
                return false;
            }
            else if (PurchasePrice == null)
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