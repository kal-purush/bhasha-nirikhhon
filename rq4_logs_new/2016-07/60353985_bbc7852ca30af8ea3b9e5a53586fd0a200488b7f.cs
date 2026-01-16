using System;


namespace GameOne.Source.Factories
{
    using Enumerations;
    using World;
    using Entities;

    public class EnemyFactory
    {
        public static Enemy MakeEnemy(double x, double y, EnemyType type, int difficulty)
        {
            double direction = Math.Round(Math.PI / 2 * LevelMaker.Rand(4), 2);
            int HP = 0;
            int damage = 0;
            switch (type)
            {
                case EnemyType.Zombie:
                    HP = 50;
                    damage = 3;
                    break;
                case EnemyType.Sentry:
                    HP = 30;
                    damage = 6;
                    break;
            }
            damage += (int) (damage * difficulty / 3.0);
            HP += (int) (HP * difficulty / 12.0);
            Enemy enemy = new Enemy(x, y, direction, 0.3, null, HP, damage, AttackType.Melee, type, HP);
            return enemy;
        }
    }
}