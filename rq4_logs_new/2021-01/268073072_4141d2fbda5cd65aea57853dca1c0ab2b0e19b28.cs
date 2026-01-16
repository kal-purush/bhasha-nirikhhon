using System;

namespace Public.DTO.Reservation
{
    public class ReservationExtrasDTO
    {
        public Guid Id { get; set; }
        public Guid ReservationId { get; set; }
        public ReservationDTO? Reservation  { get; set; }
        public Guid ExtraId { get; set; }
        public ExtraDTO? Extra { get; set; }
    }
}