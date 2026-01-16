using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeeEnDee
{
    public abstract class Entity
    {
        public string Name { get; set; }
        public string Race { get; set; }
        public string Size { get; set; }
        public string Description { get; set; }

        public int Strength { get; set; }
        public int Dexterity { get; set; }
        public int Constitution { get; set; }
        public int Intelligence {get;set;}
        public int Wisdom { get; set; }
        public int Charisma { get; set; }

        public int Health { get; set; }
        public int AC { get; set; }
        public int Speed { get; set; }
        public string Alignment { get; set; }

        public int TotalAnimalHandling { get; set; }
        public int TotalArcana { get; set; }
        public int TotalAcrobatics { get; set; }
        public int TotalAthletics { get; set; }
        public int TotalDeception { get; set; }
        public int TotalHistory { get; set; }
        public int TotalInsight { get; set; }
        public int TotalIntimidation { get; set; }
        public int TotalInvestigation { get; set; }
        public int TotalMedicine { get; set; }
        public int TotalNature { get; set; }
        public int TotalPerception { get; set; }
        public int TotalPerformance { get; set; }
        public int TotalPersuasion { get; set; }
        public int TotalReligion { get; set; }
        public int TotalSleightOfHand { get; set; }
        public int TotalStealth { get; set; }
        public int TotalSurvival { get; set; }

        public string DamageVulnerability { get; set; }
        public string DamageResistance { get; set; }
        public string Senses { get; set; }
        public override string ToString()
        {
            return "Entity: " + Name + '(' + Race + ')';
        }
    }
}