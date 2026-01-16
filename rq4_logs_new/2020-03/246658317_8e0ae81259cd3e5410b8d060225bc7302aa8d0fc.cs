using System;
using System.Collections.Generic;

using StrategyGame.Warriors.Abstractions;

using static ITEA_Collections.Common.Extensions;

namespace StrategyGame.Actions
{
    public class Battle<A1, A2> where A1 : CombatUnit, new()
                                where A2 : CombatUnit, new()
    {
        protected IEnumerable<A1> army1;
        protected IEnumerable<A2> army2;

        protected Battle() { }

        public Battle(IEnumerable<A1> army1, IEnumerable<A2> army2)
        {
            this.army1 = army1;
            this.army2 = army2;
        }

        public virtual string CountResults()
        {
            ToConsole("Battle", ConsoleColor.Cyan);
            ToConsole($"Type: {typeof(A1).Name}");
            var totalPoints1 = CountPoints(army1);
            ToConsole($"Type: {typeof(A2).Name}");
            var totalPoints2 = CountPoints(army2);
            return CountResults(totalPoints1, totalPoints2);
        }

        public virtual string CountResults(int totalPoints1, int totalPoints2)
        {
            int result = totalPoints1 - totalPoints2;
            if (result > 3) return $"First army ({typeof(A1).Name}) win!";
            else if (result < -3) return $"Second army ({typeof(A2).Name}) win!";
            else return "Draw.";
        }

        protected virtual int CountPoints(IEnumerable<CombatUnit> army)
        {
            int total = 0;
            int count = 0;
            foreach (var item in army)
            {
                count++;
                total += item.Health + (int)(item.Strength * (item.UnitType == CombatUnitType.Melee ? 1 : 1.5));
            }
            ToConsole($"Total army count: {count}", ConsoleColor.DarkYellow);
            ToConsole($"Total army strength: {total}", ConsoleColor.DarkYellow);
            return total;
        }
    }
}