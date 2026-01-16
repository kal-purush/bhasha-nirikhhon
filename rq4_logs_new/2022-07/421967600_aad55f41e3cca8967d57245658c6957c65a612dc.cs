using PlutoDAO.Gov.Application.Extensions;
using PlutoDAO.Gov.Application.Proposals.Responses;
using System.Collections.Generic;

namespace PlutoDAO.Gov.Application.Dtos
{
    public class ListResponseDto : ILinkedResource
    {
        public int CurrentPage { get; set; }
        public int TotalItems { get; set; }
        public int TotalPages { get; set; }
        public List<ProposalIdentifier> Items { get; set; }
        public IDictionary<LinkedResourceType, LinkedResource> Links { get; set; }
    }
}