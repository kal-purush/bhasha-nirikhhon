using System.Collections.Generic;
using System.Linq;
using SpringChallenge2022.Common.Services;

namespace SpringChallenge2022.Models
{
    public class Player
    {
        private int _health;
        private int _mana;

        public Dictionary<int, Hero> Heroes { get; } = new Dictionary<int, Hero>();

        public Vector BasePosition { get; }

        public Player(Vector basePosition)
        {
            BasePosition = basePosition;
        }

        public void Update(int health, int mana, IReadOnlyList<Monster> monsters)
        {
            _health = health;
            _mana = mana;

            foreach (var hero in Heroes.Values)
            {
                if (hero.TargetedMonster != null && monsters.All(x => x.Id != hero.TargetedMonster.Id))
                {
                    Io.Debug($"removing {hero.TargetedMonster.Id} from {hero.Id}");
                    hero.TargetedMonster = null;
                }
            }
        }
    }
}