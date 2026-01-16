using System;
using System.Collections.Generic;
using System.Text;

namespace HotelBooking.Application.Queries
{
    public class BookingQueryParameters : QueryParameters
    {
        public int dayStart { get; set; }
        public int monthStart { get; set; }
        public int yearStart { get; set; }
        public int dayEnd { get; set; }
        public int monthEnd { get; set; }
        public int yearEnd { get; set; }
        public int UserId { get; set; }
        public int HotelId { get; set; }
        public int RoomId { get; set; }
    }
}