using System;
using System.Collections.Generic;
using System.Text;

namespace CourseGenerator.Models.Info.Entities
{
    public class Competency
    {
        public int Id { get; set; }
        public string Note { get; set; }

        public int? ParentId { get; set; }
        public Competency Parent { get; set; }
        public ICollection<Competency> Competencies { get; set; }

        public int LevelId { get; set; }
        public Level Level { get; set; }

        public ICollection<HeadingCompetency> HeadingCompetencies { get; set; }
        public ICollection<MaterialCompetency> MaterialCompetencies { get; set; }
    }
}