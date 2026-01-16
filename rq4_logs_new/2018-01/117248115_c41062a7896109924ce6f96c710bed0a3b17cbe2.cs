using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace TBGResearch.Classes
{
    public class ProgressFrame
    {
        /// <summary>
        /// Identifying detail
        /// </summary>
        public string IdTag { get; set; }

        public bool IsComplete { get; set; }

        public bool IsAccessible { get; set; }

        public bool IsActive { get; set; }

        public string CurrentTechTeamId { get; set; }

        [JsonIgnore]
        public TechTeam CurrentTechTeam { get; set; }

        private List<ComponentProgressLine> _lines = new List<ComponentProgressLine>();
        public List<ComponentProgressLine> Lines
        {
            get
            {
                return _lines;
            }
            set
            {
                _lines = value;
            }
        }
    }
}