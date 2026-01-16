using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace TBGResearch.Classes
{
    public class Entity
    {
        /// <summary>
        /// Name of this Nation/Species/Power
        /// </summary>
        public string Name { get; set; }

        private List<TechTeam> _Teams = new List<TechTeam>();
        /// <summary>
        /// A List of Tech Teams that belong to this power
        /// </summary>        
        public List<TechTeam> Teams
        {
            get
            {
                return _Teams;
            }
            set
            {
                _Teams = value;
            }
        }

        /// <summary>
        /// The current research progress of this Entity
        /// </summary>
        public EntityStateOfArt Status { get; set; }

        private Dictionary<TechTeam, ResearchFrame> _Assignments = new Dictionary<TechTeam, ResearchFrame>();
        /// <summary>
        /// Stores ongoing TechTeam Assignments
        /// </summary>
        public Dictionary<TechTeam, ResearchFrame> Assignments
        {
            get
            {
                return _Assignments;
            }
            set
            {
                _Assignments = value;
            }
        }

        // === Methods ===

        public string Serialise()
        {
            return JsonConvert.SerializeObject(this);
        }

        public static void Deserialise(string json)
        {
            JsonConvert.DeserializeObject<Entity>(json);
        }
    }
}