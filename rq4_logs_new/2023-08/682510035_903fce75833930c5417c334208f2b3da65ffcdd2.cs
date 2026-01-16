using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FirstWebAPI.Entities
{
    public class OrderEntity
    {
        public int OrderId { get; set; } // Key
        public DateTime OrderDate { get; set; }
        public DateTime? DeliveryDate { get; set; }
        public double TotalPrice { get; set; }
        public int TotalQuantity { get; set; }
        public int Voucher { get; set; }

        public ICollection<OrderDetailEntity> orderDetails;

        public OrderEntity()
        {
            orderDetails = new HashSet<OrderDetailEntity>();
        }
    }
}