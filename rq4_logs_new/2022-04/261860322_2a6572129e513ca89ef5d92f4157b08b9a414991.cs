using System;
using System.Collections.Generic;
using System.Linq;
using SpringChallenge2022.Actions;
using SpringChallenge2022.Models;

namespace SpringChallenge2022.Agents
{
    internal class AgentWood1Boss
    {
        public IReadOnlyList<IAction> GetAction(Game game)
        {
            var actions = new List<IAction>();

            var rankedMonsters = new List<Tuple<int, Entity>>();

            foreach (var monster in game.Monsters)
            {
                var threatLevel = 0;

                if (monster.TargetingBase && monster.ThreatFor == 1)
                {
                    threatLevel = 1000;
                }
                else if (monster.ThreatFor == 1)
                {
                    threatLevel = 500;
                }

                var distance = game.MyBase.Position.GetDistanceSquared(monster.Position);
                var distanceScore = 500 * (1 / distance + 1);

                rankedMonsters.Add(new Tuple<int, Entity>(threatLevel + distanceScore, monster));
            }

            rankedMonsters = rankedMonsters.OrderByDescending(x => x.Item1).ToList();

            for (var index = 0; index < game.MyHeroes.Count; index++)
            {
                if (rankedMonsters.Count > index)
                {
                    var monster = rankedMonsters[index].Item2;
                    actions.Add(new MoveAction(monster.Position));
                }
                else
                {
                    actions.Add(new WaitAction());
                }
            }

            return actions;
        }
    }
}