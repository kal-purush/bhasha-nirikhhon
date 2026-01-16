using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TripEnjoy.Application.Data
{
    public class BookingOfPartnerResponse
    {
      
        public int BookingId { get; set; }

        // thông tin khách hàng booking
        public string? AccountEmail { get; set; }

        public string? AccountFullName { get; set; }

        public string? AccountPhoneNumber { get; set; }

        public DateTime CheckinDate { get; set; }  
        public DateTime CheckoutDate { get; set; }

        // tình trạng phòng đã booking
        public string? BookingStatus { get; set; }
        public int RoomQuantity { get; set; }

        // Thong tin khach san dang ky
        public int HotelId { get; set; }
        public string? HotelName { get; set; }
        // thông tin phòng đã booking
        public int RoomId { get; set; }
        public string? RoomName { get; set; }

        // thông tin voucher đã sử dụng
        public int VoucherId { get; set; } 
      
     // giá trị đơn booking của khách hàng
        public decimal BookingTotalPrice { get; set; }
    }
}