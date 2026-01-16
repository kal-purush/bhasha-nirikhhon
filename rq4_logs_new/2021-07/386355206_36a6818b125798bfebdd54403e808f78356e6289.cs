using System;

namespace ClassesTasks
{
    public class Fighter
    {
        public string Name;
        public int Health, DamagePerAttack;

        public Fighter(string name, int health, int damagePerAttack)
        {
            this.Name = name;
            this.Health = health;
            this.DamagePerAttack = damagePerAttack;
        }

        public string DeclareWinner(Fighter fighter1, Fighter fighter2, string firstAttacker)
        {
            // if (firstAttacker == fighter2.Name)
            // {
            //     while (fighter1.Health > 0 && fighter2.Health > 0)
            //     {
            //         fighter1.Health = fighter1.Health - fighter2.DamagePerAttack;
            //         if (fighter1.Health <= 0) return fighter2.Name;
            //         fighter2.Health = fighter2.Health - fighter1.DamagePerAttack;
            //         if (fighter2.Health <= 0) return fighter1.Name;
            //     }
            // }
            //
            // else
            // {
            //     while (fighter1.Health > 0 && fighter2.Health > 0)
            //     {
            //         fighter2.Health = fighter2.Health - fighter1.DamagePerAttack;
            //         if (fighter2.Health <= 0) return fighter1.Name;
            //         fighter1.Health = fighter1.Health - fighter2.DamagePerAttack;
            //         if (fighter1.Health <= 0) return fighter2.Name;
            //     }
            // }
            //
            // return firstAttacker;

            int FirstFighter = (fighter1.Health - 1) / fighter2.DamagePerAttack;
            int SecondFighter = (fighter2.Health - 1) / fighter1.DamagePerAttack;
    
            return FirstFighter > SecondFighter ? fighter1.Name : SecondFighter > FirstFighter ? fighter2.Name : firstAttacker;
        }
    }
}