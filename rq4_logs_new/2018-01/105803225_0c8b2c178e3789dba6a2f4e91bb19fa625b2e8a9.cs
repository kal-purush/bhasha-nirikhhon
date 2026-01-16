using System.Collections.Generic;

namespace Fsd.Pzpn.Crew.Api.Dtos
{
    public class ClubSummaryDto
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public IEnumerable<PlayerSummaryDto> Players { get; set; }
    }
}

