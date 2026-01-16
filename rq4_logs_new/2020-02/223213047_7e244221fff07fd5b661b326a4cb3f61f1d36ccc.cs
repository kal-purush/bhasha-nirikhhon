using System;

namespace DFC.App.MatchSkills.Services.ServiceTaxonomy.Models
{
    public class StOccupationSkills
    {

            public StOsSkill[] Skills { get; set; }
            public string Occupation { get; set; }
            public DateTime LastModified { get; set; }
            public string[] AlternativeLabels { get; set; }
            public string Uri { get; set; }


            public class StOsSkill
            {
                public string RelationshipType { get; set; }
                public string Skill { get; set; }
                public DateTime LastModified { get; set; }
                public string[] AlternativeLabels { get; set; }
                public string Type { get; set; }
                public string Uri { get; set; }
                public string SkillReusability { get; set; }
            }
    }
}