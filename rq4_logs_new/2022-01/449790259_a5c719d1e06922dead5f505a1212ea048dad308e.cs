using System;
using System.Collections.Generic;
using System.Text;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json.Serialization;

namespace HotelBooking.Domain.Entities
{
    [Table("Bookings")]
    public class BookingEntity : AuditableEntity
    {
        [Key]
        public int BookingId { get; set; }
        public DateTime BookingDate { get; set; }
        public int UserId { get; set; }
        public int HotelId { get; set; }
        public int RoomId { get; set; }
        [JsonIgnore]
        public virtual UserEntity User { get; set; }
        [JsonIgnore]
        public virtual HotelEntity Hotel { get; set; }
    }
}