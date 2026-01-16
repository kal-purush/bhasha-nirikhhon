using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace DFC.App.MatchSkills.Services.ServiceTaxonomy.Models
{
    public class StLabelSkills
    {
        public StLabelSkill[] Skills { get; set; }
    }

    public class StLabelSkill
    {
        public string Uri { get; set; }
        public string Skill { get; set; }
        public string[] AlternativeLabels { get; set; }
        public DateTime LastModified { get; set; }
        public string SkillType { get; set; }
        public string SkillReusability { get; set; }
        public string RelationshipType { get; set; }
        public Matches Matches { get; set; }
    }
    public class Matches
    {
        public string[] Skill { get; set; }
        public string[] AlternativeLabels { get; set; }
        public string[] HiddenLabels { get; set; }
    }
}