using System;
using Freddy.Application.Commands.Products;

namespace Freddy.Persistence.OrderItems
{
    public class OrderItemEntity
    {
        public Product Product { get; set; }
        public decimal NettoPrice { get; set; }
        public double TaxPercentage { get; set; }
        public string Note { get; set; }
        public ItemStatus Status { get; set; }

        [Flags]
        public enum ItemStatus
        {
            Ordered = 0b_0000_0001,
            Unavailable = 0b_0000_0010,
            Delivered = 0b_0000_0100
        }
    }
}