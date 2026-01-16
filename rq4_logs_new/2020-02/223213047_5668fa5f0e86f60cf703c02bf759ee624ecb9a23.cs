using System;

namespace DFC.App.MatchSkills.Application.ServiceTaxonomy.Models
{
    public class OccupationMatch
    {
        public string Uri { get; set; }

        public string JobProfileUri { get; set; }

        public string JobProfileTitle { get; set; }

        public int TotalOccupationEssentialSkills { get; set; }

        public int TotalOccupationOptionalSkills { get; set; }

        public int MatchingEssentialSkills { get; set; }

        public int MatchingOptionalSkills { get; set; }

        public DateTime LastModified { get; set; }
    }
}