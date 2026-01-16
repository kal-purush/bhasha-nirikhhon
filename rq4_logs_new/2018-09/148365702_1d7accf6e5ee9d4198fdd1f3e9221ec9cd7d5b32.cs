using System;

namespace MassTransit_Saga.Contracts
{
    public class BookCreated: IBookCreated
    {
        public Guid CorrelationId { get; set; }
        public string ContactFirstName { get; set; }
        public string ContactLastName { get; set; }
        public string ContactEmail { get; set; }
        public string ContactPhone { get; set; }
        public DateTimeOffset ArrivalTime { get; set; }
        public DateTimeOffset DepartureTime { get; set; }
        public int RatePlanId { get; set; }
        public int[] ObjectRoomIds { get; set; }
        public string AdditionalRequirements { get; set; }
        public DateTimeOffset Created { get; set; }
    }
}