using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace API.Helpers
{
    public class OrgLikeParams : OrganizationParams
    {
        public int UserId { get; set; }
        public int OrgId { get; set; }
        public string Predicate { get; set; }
    }
}