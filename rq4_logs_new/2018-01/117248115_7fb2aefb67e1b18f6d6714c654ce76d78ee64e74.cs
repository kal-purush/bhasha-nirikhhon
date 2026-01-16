using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TBGResearch.Classes;

namespace TBGResearch.Logic
{
    public static class BonusCalculator
    {
        public static int CalculateEffectiveBonus(Entity parent, TechTeam researcher, ResearchFrame frame)
        {
            int bonus = 0;

            List<ResearchBonus> applicableBonus = parent.Status.AdmiralBonuses.Where(x => x.TreeTarget == frame.Tree).ToList();
            applicableBonus.AddRange(parent.Status.AdmiralBonuses.Where(x => frame.PartContained.Contains(x.PartTarget)));

            if (applicableBonus.Count > 0)
                bonus = applicableBonus.Sum(x => x.BonusValue);

            return bonus;
        }

        public static bool MatchPreference(TechTeam researcher, ResearchFrame frame)
        {
            if (researcher.PreferredSkill1 == frame.Tree || researcher.PreferredSkill2 == frame.Tree)
                return true;
            else
                return false;
        }
    }
}