using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Domain
{
    public class IngestionService
    {

        private readonly TicketService _ticketService;
        private readonly Discovery _discovery;

        public IngestionService(TicketService ticketService, Discovery discovery)
        {
            _ticketService = ticketService;
            _discovery = discovery;
        }


        public async Task Ingest(Integration integration)
        {
            var ticket = await _ticketService.GetTicket(integration);

            var discoveryRecord = new DiscoveryRecord
            {
                Id = ticket?.Id,
                Name = ticket?.Name + "Any Transformation Rule",
                Description = ticket?.Description,
            };

            await _discovery.Load(discoveryRecord, integration);

        }
    }
}