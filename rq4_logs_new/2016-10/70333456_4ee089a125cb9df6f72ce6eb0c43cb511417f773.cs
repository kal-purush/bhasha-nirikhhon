using System;

namespace RPG
{
    public class Character
    {
        protected string name;

        protected int level;
 
        protected int health;
        protected int maxHealth;
        protected int strength;
        protected int defence;
        

        /* DEFAULT CONSTRUCTOR */
        public Character()
        {
            this.name = "";
            this.level = 1;
            this.health = 100;
            this.maxHealth = 100;
            this.strength = 1;
            this.defence = 1;
        }

        /* ALTERNATE CONSTRUCTOR */
        public Character(string name, int level, int health, int maxHealth, int strength, int defence)
        {
            this.name = name;
            this.level = level;
            this.health = health;
            this.maxHealth = maxHealth;
            this.strength = strength;
            this.defence = defence;
        }

        /* Getters */
        public string getName()
        {
            return this.name;
        }

        public int getLevel()
        {
            return this.level;
        }

        public int getHealth()
        {
            return this.health;
        }

        public int getMaxHealth()
        {
            return this.maxHealth;
        }

        public int getStrength()
        {
            return this.strength;
        }

        public int getDefence()
        {
            return this.defence;
        }

        /* Setters */
        public void setName(string name)
        {
            this.name = name;
        }

        public void setHealth(int health)
        {
            this.health = health;

            if (health <= 0)
            {
                //run death function
            }
            if (health > maxHealth)
            {
                health = maxHealth;
            }
        }

        public void setMaxHealth(int maxHealth)
        {
            this.maxHealth = maxHealth;
        }

        public void setLevel(int level)
        {
            this.level = level;
        }

        public void setStrength(int strength)
        {
            this.strength = strength;
        }

        public void setDefence(int defence)
        {
            this.defence = defence;
        }
    }

    public class Player : Character
    {
        private int experience;
        private int maxExp;
        private int money;

        public Player() : base()
        {
            this.experience = 0;
            this.maxExp = 100;
            this.money = 0;
        }

        public Player(string name, int level, int health, int maxHealth,int strength, int defence, int experience, int maxExp, int money)
        {
            this.experience = experience;
            this.maxExp = maxExp;
            this.money = money;
        }

        //Getters
        public int getExperience()
        {
            return this.experience;
        }

        public int getMoney()
        {
            return this.money;
        }

        //Setters
        public void setExperience(int experience)
        {
            this.experience = experience;
        }

        public void setMoney(int money)
        {
            this.money = money;
        }
    }
}