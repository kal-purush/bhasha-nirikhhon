using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace jsonmaker
{
    public struct Monster
    {

        public string Type { get; set; }
        public string Challenge { get; set; }
        public int PageNumber { get; set; }
        public string Size { get; set; }
        public string Appearance { get; set; }
        public string Race { get; set; }
        public string Alignment { get; set; }
        public string ArmorClass { get; set; }
        public int HitDie { get; set; }
        public int HitDieNum { get; set; }
        public string Speed { get; set; }
        public int Strength { get; set; }
        public int Dexterity { get; set; }
        public int Constitution { get; set; }
        public int Intelligence { get; set; }
        public int Wisdom { get; set; }
        public int Charisma { get; set; }
        public string DamageVulnerability { get; set; }
        public string DamageResistance { get; set; }
        public string ConditionVulnerability { get; set; }
        public string Immunity { get; set; }
        public string Description { get; set; }
        public List<string> Languages { get; set; }
        public List<string> Senses { get; set; }
        public List<string> Actions { get; set; }
        public List<string> LegendaryActions { get; set; }
        public Dictionary<string, int> skills { get; set; }
       
    }
}