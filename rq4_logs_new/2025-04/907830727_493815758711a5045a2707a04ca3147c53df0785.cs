using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EasyTravel.Domain.Entites
{
    public class Booking : IEntity<Guid>
    {
        public Guid Id { get; set; }
        public BookingStatus BookingStatus { get; set; } // e.g., Confirmed, Cancelled, Completed
        public BookingTypes BookingTypes { get; set; }
        public decimal TotalAmount { get; set; }
        public Guid UserId { get; set; }
        public User? User { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime UpdatedAt { get; set; } 
        public List<Payment>? Payments { get; set; } // List of payments associated with the booking
        public HotelBooking? HotelBooking { get; set; } // Navigation property to the hotel booking entity
        public BusBooking? BusBooking { get; set; } // Navigation property to the bus booking entity
        public PhotographerBooking? PhotographerBooking { get; set; } // Navigation property to the photographer booking entity
        public GuideBooking? GuideBooking { get; set; } // Navigation property to the guide booking entity
        public CarBooking? CarBooking { get; set; } // Navigation property to the car booking entity
    }
}