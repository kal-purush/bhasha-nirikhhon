using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EasyTravel.Domain.Entites
{
    public class Payment : IEntity<Guid>
    {
        public Guid Id { get; set; }
        public string PaymentMethod { get; set; } // e.g., Credit Card, PayPal
        public decimal Amount { get; set; }
        public DateTime PaymentDate { get; set; }
        public string Status { get; set; } // e.g., Completed, Pending, Failed
        public Guid BookingId { get; set; } // Foreign key to the booking entity
        //public Booking Booking { get; set; } // Navigation property to the booking entity
    }
}