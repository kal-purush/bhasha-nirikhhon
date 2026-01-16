using System;
using System.Collections.Generic;
using System.Linq;
using SpringChallenge2022.Actions;
using SpringChallenge2022.Common.Services;
using SpringChallenge2022.Models;

namespace SpringChallenge2022.Agents
{
    internal class SilverBoss
    {
        private Dictionary<int, IAction> _actions;
        private int _availableMana;
        private Game _game;

        public IReadOnlyList<IAction> GetAction(Game game)
        {
            _game = game;
            _actions = new Dictionary<int, IAction>();
            _availableMana = game.MyPlayer.Mana;

            foreach (var monster in _game.Monsters.Values)
            {
                Io.Debug($"MonsterID: {monster.Id}");
            }

            var rankedMonsters = GetRankedMonsters();

            if (rankedMonsters.Count > Constants.NumberOfHeroes)
            {
                AddSpellActionIfTooManyMonsters(rankedMonsters);
            }

            Io.Debug($"RankedMonsters {rankedMonsters.Count}");

            foreach (var rankedMonster in rankedMonsters)
            {
                Io.Debug($"Evaluating {rankedMonster}");

                var heroTargetingMonster = game.MyPlayer.Heroes.Values.SingleOrDefault(x => x.TargetedMonster?.Id == rankedMonster.Monster.Id);

                if (heroTargetingMonster != null)
                {
                    var action = GetActionIfHeroAlreadyTargetingMonster(heroTargetingMonster, rankedMonster);
                    _actions.Add(heroTargetingMonster.Id, action);
                }
                else
                {
                    var hero = GetHeroToTargetMonster(rankedMonster);

                    if (hero != null)
                    {
                        hero.TargetedMonster = rankedMonster.Monster;
                        Io.Debug($"hero {hero.Id} targeting {hero.TargetedMonster.Id}");
                        _actions.Add(hero.Id, new MoveAction(rankedMonster.Monster.Position));
                    }
                }
            }

            AddActionsForHeroesWithoutAction(rankedMonsters.Select(x => x.Monster).ToList());

            return _actions.OrderBy(x => x.Key).Select(x => x.Value).ToList();
        }

        private IReadOnlyList<RankedMonster> GetRankedMonsters()
        {
            var rankedMonsters = new List<RankedMonster>();

            foreach (var monster in _game.Monsters.Values)
            {
                if (monster.ThreatFor == 1)
                {
                    var turnsToReach = monster.GetTurnsToReach(_game.MyPlayer.BasePosition);
                    var shotsNeeded = monster.GetHitsNeeded();

                    var distanceScore = Constants.DistanceBaseScore / (turnsToReach + 1);
                    var shotsNeededScore = shotsNeeded * Constants.ShotsNeededBaseScore;

                    var baseThreatLevel = monster.TargetingBase
                        ? Constants.TargetingBaseBaseScore
                        : Constants.NonTargetingBaseBaseScore;

                    var threatLevel = baseThreatLevel + distanceScore + shotsNeededScore;

                    Io.Debug($"Scores : threatLevel {threatLevel} : baseThreatLevel {baseThreatLevel} : distanceScore {distanceScore} : shotsNeededScore {shotsNeededScore}");

                    rankedMonsters.Add(
                        new RankedMonster(
                            monster,
                            threatLevel,
                            turnsToReach,
                            shotsNeeded));
                }
            }

            return rankedMonsters.OrderByDescending(x => x.ThreatLevel).ToList();
        }

        private bool IsMonsterInRange(
            Hero hero,
            Monster monster,
            int range,
            int minDistance)
        {
            var distance = (hero.Position - monster.Position).Length();
            return distance < range && distance > minDistance;
        }

        private IAction GetActionIfHeroAlreadyTargetingMonster(Hero hero, RankedMonster targetedMonster)
        {
            Io.Debug($"hero {hero.Id} already targeted {targetedMonster.Monster.Id}");

            var spellAction = GetSpellAction(hero, targetedMonster);

            return spellAction ?? new MoveAction(targetedMonster.Monster.Position);
        }

        private IAction? GetSpellAction(Hero hero, RankedMonster targetedMonster)
        {
            if (CanCastWindSpell(hero, targetedMonster))
            {
                return GetSpellAction(SpellType.Wind, null);
            }
            else if (CanCastControlSpell(hero, targetedMonster.Monster))
            {
                var distance = (targetedMonster.Monster.Position - _game.MyPlayer.BasePosition).Length();

                if (distance > Constants.BaseRadius)
                {
                    return GetSpellAction(SpellType.Control, targetedMonster.Monster);
                }
            }

            return null;
        }

        private IAction GetSpellAction(SpellType spellType, Monster? monster)
        {
            _availableMana -= Constants.ManaRequiredForSpell;

            switch (spellType)
            {
                case SpellType.Wind:
                    return new WindSpellAction(_game.OpponentPlayer.BasePosition);
                case SpellType.Control:
                    monster.SetControlled();
                    return new ControlSpellAction(monster.Id, _game.OpponentPlayer.BasePosition);
                case SpellType.Shield:
                default:
                    throw new ArgumentOutOfRangeException(nameof(spellType), spellType, null);
            }
        }

        private bool CanCastWindSpell(Hero hero, RankedMonster rankedMonster)
            => _availableMana >= Constants.ManaRequiredForSpell
               && rankedMonster.Monster.TargetingBase
               && IsMonsterInRange(
                   hero,
                   rankedMonster.Monster,
                   Constants.WindSpellRange,
                   0);

        private bool CanCastControlSpell(Hero hero, Monster rankedMonster)
            => _availableMana >= Constants.ManaRequiredForSpell
               && IsMonsterInRange(
                   hero,
                   rankedMonster,
                   Constants.ControlSpellRange,
                   Constants.HeroDamageDistance)
               && !rankedMonster.ControlledByMe;

        private Hero GetHeroToTargetMonster(RankedMonster rankedMonster)
        {
            Hero bestHero = null;
            var bestHeroDistance = int.MaxValue;

            foreach (var hero in _game.MyPlayer.Heroes.Values)
            {
                if (_actions.ContainsKey(hero.Id))
                {
                    continue;
                }

                var distance = (int)(hero.Position - rankedMonster.Monster.Position).LengthSquared();

                if (distance < bestHeroDistance)
                {
                    bestHeroDistance = distance;
                    bestHero = hero;
                }
            }

            return bestHero;
        }

        private void AddActionsForHeroesWithoutAction(IReadOnlyList<Monster> rankedMonsters)
        {
            var monstersForWildMana = _game.Monsters.Values.Where(monster => !monster.ControlledByMe && IsMonsterValidForWildMana(monster)).ToList();

            foreach (var hero in _game.MyPlayer.Heroes.Values)
            {
                if (!_actions.ContainsKey(hero.Id))
                {
                    var action = GetActionIfDoingNothing(hero, rankedMonsters, monstersForWildMana);

                    _actions.Add(hero.Id, action);
                }
            }
        }

        private IAction GetActionIfDoingNothing(Hero hero, IReadOnlyList<Monster> rankedMonsters, IReadOnlyList<Monster> monstersForWildMana)
        {
            Monster? bestMonster;

            if (monstersForWildMana.Any())
            {
                bestMonster = GetClosestMonster(hero, monstersForWildMana);

                if (bestMonster != null)
                {
                    Io.Debug($"Moving hero {hero.Id} to Monster {bestMonster.Id} for Wild Mana");
                    return new MoveAction(bestMonster.Position);
                }
            }

            bestMonster = GetClosestMonster(hero, rankedMonsters);

            if (bestMonster != null)
            {
                if (CanCastControlSpell(hero, bestMonster))
                {
                    return GetSpellAction(SpellType.Control, bestMonster);
                }

                return new MoveAction(bestMonster.Position);
            }

            return new MoveAction(hero.StartingPosition);
        }

        private static Monster? GetClosestMonster(Hero hero, IReadOnlyList<Monster> monsters)
        {
            Monster bestMonster = null;
            var bestMonsterDistance = int.MaxValue;

            foreach (var monster in monsters)
            {
                var distance = (int)(hero.Position - monster.Position).LengthSquared();

                if (distance < bestMonsterDistance)
                {
                    bestMonsterDistance = distance;
                    bestMonster = monster;
                }
            }

            return bestMonster;
        }

        private void AddSpellActionIfTooManyMonsters(IReadOnlyList<RankedMonster> rankedMonsters)
        {
            foreach (var hero in _game.MyPlayer.Heroes.Values)
            {
                if (rankedMonsters.Any(rankedMonster => CanCastWindSpell(hero, rankedMonster)))
                {
                    Io.Debug($"removing {hero.TargetedMonster?.Id} from {hero.Id} because of wind action");
                    hero.TargetedMonster = null;
                    _actions.Add(hero.Id, GetSpellAction(SpellType.Wind, null));
                    break;
                }
            }
        }

        private bool IsMonsterValidForWildMana(Monster monster)
        {
            var distance = GetDistanceFromBase(monster);
            return distance > Constants.BaseRadius && distance < Constants.MaxDistanceFromBaseForHero;
        }

        private float GetDistanceFromBase(Monster monster)
        {
            var distance = (monster.Position - _game.MyPlayer.BasePosition).Length();
            Io.Debug($"Monster Id {monster.Id} :  Distance {distance}");
            return distance;
        }
    }
}