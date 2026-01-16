using System;

namespace DFC.App.MatchSkills.Services.ServiceTaxonomy.Models
{
    public class SkillsGapAnalysis
    {
        public string Occupation { get; set; }

        public StOccupationSkills.StOsSkill[] MissingSkills { get; set; }

        public string JobProfileTitle { get; set; }

        public StOccupationSkills.StOsSkill[] MatchingSkills { get; set; }

        public DateTimeOffset LastModified { get; set; }

        public string[] AlternativeLabels { get; set; }

        public Uri JobProfileUri { get; set; }
        public Uri Uri { get; set; }
    }

}
