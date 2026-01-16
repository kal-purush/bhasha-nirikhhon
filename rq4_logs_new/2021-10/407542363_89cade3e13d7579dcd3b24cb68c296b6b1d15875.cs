using System;

namespace Library
{
    public class Wizard : CharacterWizard
    {
        public Array[] Items {get; private set;}
        public Array[] Elements {get; private set;}
        public int health { get; set; }

        public Wizard (int id, string name)
        {
            this.Id = id;
            this.Name = name;
            this.Health = 150;
            this.AttackValue = 20;
            this.DefenseValue = 80;
        }  

    }
}