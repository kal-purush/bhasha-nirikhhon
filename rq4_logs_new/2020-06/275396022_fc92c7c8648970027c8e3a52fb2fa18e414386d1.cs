using System;
using System.Collections.Generic;
using System.Text;

namespace OOPgame1.Actors
{
    internal class MainHero
    {
        Inventory inventory;

        private static MainHero instance;

        private MainHero(int h, int d)
        {

            _health = h;
            _damage = d;

            Inventory inv = Inventory.getInstance();
            this.inventory = inv;
            Console.WriteLine("inventory created");

        }

        public static MainHero getInstance(int h, int d)
        {
            if (instance == null)
            {
                instance = new MainHero(h, d);
            }
            return instance;
        }


        private int _health;
        public int Health
        {
            get
            {
                return _health;
            }
            set
            {
                _health = value;
            }
        }


        private int _damage;
        public int Damage
        {
            get
            {
                return _damage;
            }
            set
            {
                _damage = value;
            }

        }


    }
}