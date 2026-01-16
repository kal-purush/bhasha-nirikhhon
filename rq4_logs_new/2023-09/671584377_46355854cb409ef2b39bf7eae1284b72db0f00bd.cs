using System.Text.Json.Serialization;
using Ticketinho.Common.Enums;

namespace Ticketinho.Common.DTOs.Demand
{
    public class CreateDemandRequestDto
    {
        public string UserId { get; set; }

        public double Price { get; set; }

        public TicketZone Zone { get; set; }

        public TicketType Type { get; set; }
    }
}