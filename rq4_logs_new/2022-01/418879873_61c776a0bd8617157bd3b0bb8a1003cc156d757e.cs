using System;
using System.Collections.Generic;
using RPGDatabase.Models.Character;
using RPGDatabase.Models.Enum;
using RPGDatabase.Models.Item;

namespace RPGEditor.StaticListAdd
{
    public static class DataToImportAndCreate
    {
        public static List<Monster> CreateMonster()
        {
            List<Monster> monsters = new List<Monster>();
            for (int i = 1; i < 6; i++)
            {
                Monster monster = new Monster("Bébé dragon d'eau", TypeEnum.WATER);
                monster.Level = i;
                monster.PvMax = 60 * i;
                monster.Pv = monster.PvMax;
                monster.Stats.Power = 5 * i;
                monster.Stats.Dexterity = 3 * i;
                monster.Money = 10 * i;
                monster.ExperienceLvl = 7 * i;
                Monster dragon = new Monster("Dragon d'eau", TypeEnum.WATER);
                dragon.Level = i;
                dragon.PvMax = 90 * i;
                dragon.Pv = dragon.PvMax;
                dragon.Stats.Power = 6 * i;
                dragon.Stats.Dexterity = 6 * i;
                dragon.Money = 15 * i;
                dragon.ExperienceLvl = 10 * i;
                Monster dragonBoss = new Monster("Dragon Boss d'eau", TypeEnum.WATER);
                dragonBoss.Level = i;
                dragonBoss.PvMax = 110 * i;
                dragonBoss.Pv = dragonBoss.PvMax;
                dragonBoss.Stats.Power = 10 * i;
                dragonBoss.Stats.Dexterity = 5 * i;
                dragonBoss.Money = 25 * i;
                dragonBoss.ExperienceLvl = 20 * i;
                monsters.Add(monster);
                monsters.Add(dragon);
                monsters.Add(dragonBoss);
            }
            for (int i = 1; i < 6; i++)
            {
                Monster monster = new Monster("Bébé dragon de feu", TypeEnum.FIRE);
                monster.Level = i;
                monster.PvMax = 60 * i;
                monster.Pv = monster.PvMax;
                monster.Stats.Power = 5 * i;
                monster.Stats.Dexterity = 3 * i;
                monster.Money = 10 * i;
                monster.ExperienceLvl = 7 * i;
                Monster dragon = new Monster("Dragon de feu", TypeEnum.FIRE);
                dragon.Level = i;
                dragon.PvMax = 90 * i;
                dragon.Pv = dragon.PvMax;
                dragon.Stats.Power = 6 * i;
                dragon.Stats.Dexterity = 6 * i;
                dragon.Money = 15 * i;
                dragon.ExperienceLvl = 10 * i;
                Monster dragonBoss = new Monster("Dragon Boss de feu", TypeEnum.FIRE);
                dragonBoss.Level = i;
                dragonBoss.PvMax = 110 * i;
                dragonBoss.Pv = dragonBoss.PvMax;
                dragonBoss.Stats.Power = 10 * i;
                dragonBoss.Stats.Dexterity = 5 * i;
                dragonBoss.ExperienceLvl = 20 * i;
                dragonBoss.Money = 25 * i;
                monsters.Add(monster);
                monsters.Add(dragon);
                monsters.Add(dragonBoss);
            }
            for (int i = 1; i < 6; i++)
            {
                Monster monster = new Monster("Bébé dragon de roche", TypeEnum.ROCK);
                monster.Level = i;
                monster.PvMax = 60 * i;
                monster.Pv = monster.PvMax;
                monster.Stats.Power = 5 * i;
                monster.Stats.Dexterity = 3 * i;
                monster.Money = 10 * i;
                monster.ExperienceLvl = 7 * i;
                Monster dragon = new Monster("Dragon de roche", TypeEnum.ROCK);
                dragon.Level = i;
                dragon.PvMax = 90 * i;
                dragon.Pv = dragon.PvMax;
                dragon.Stats.Power = 6 * i;
                dragon.Stats.Dexterity = 6 * i;
                dragon.Money = 15 * i;
                dragon.ExperienceLvl = 10 * i;
                Monster dragonBoss = new Monster("Dragon Boss de roche", TypeEnum.ROCK);
                dragonBoss.Level = i;
                dragonBoss.PvMax = 110 * i;
                dragonBoss.Pv = dragonBoss.PvMax;
                dragonBoss.Stats.Power = 10 * i;
                dragonBoss.Stats.Dexterity = 5 * i;
                dragonBoss.Money = 25 * i;
                dragonBoss.ExperienceLvl = 20 * i;
                monsters.Add(monster);
                monsters.Add(dragon);
                monsters.Add(dragonBoss);
            }
            for (int i = 1; i < 6; i++)
            {
                Monster monster = new Monster("Bébé dragon de vent", TypeEnum.WIND);
                monster.Level = i;
                monster.PvMax = 60 * i;
                monster.Pv = monster.PvMax;
                monster.Stats.Power = 5 * i;
                monster.Stats.Dexterity = 3 * i;
                monster.Money = 10 * i;
                monster.ExperienceLvl = 7 * i;
                Monster dragon = new Monster("Dragon de vent", TypeEnum.WIND);
                dragon.Level = i;
                dragon.PvMax = 90 * i;
                dragon.Pv = dragon.PvMax;
                dragon.Stats.Power = 6 * i;
                dragon.Stats.Dexterity = 6 * i;
                dragon.Money = 15 * i;
                dragon.ExperienceLvl = 10 * i;
                Monster dragonBoss = new Monster("Dragon Boss de vent", TypeEnum.WIND);
                dragonBoss.Level = i;
                dragonBoss.PvMax = 110 * i;
                dragonBoss.Pv = dragonBoss.PvMax;
                dragonBoss.Stats.Power = 10 * i;
                dragonBoss.Stats.Dexterity = 5 * i;
                dragonBoss.Money = 25 * i;
                dragonBoss.ExperienceLvl = 20 * i;
                monsters.Add(monster);
                monsters.Add(dragon);
                monsters.Add(dragonBoss);
            }
            return monsters;
        }

        public static List<Consomable> CreateConsomable()
        {
            List<Consomable> consomables = new List<Consomable>();
            for (int i = 1; i < 6; i++)
            {
                Consomable consomable = new Consomable("Pomme", 10 * i, TypeEnum.FIRE, 10 * i);
                consomable.Level = i;
                consomables.Add(consomable);
                Consomable banane = new Consomable("Banane", 5 * i, TypeEnum.FIRE, 5 * i);
                banane.Level = i;
                consomables.Add(banane);
                Consomable soupe = new Consomable("Soupe", 15 * i, TypeEnum.FIRE, 15 * i);
                soupe.Level = i;
                consomables.Add(soupe);
            }
            return consomables;
        }

        public static List<Weapon> CreateWeapon()
        {
            List<Weapon> weapons = new List<Weapon>();
            for (int i = 1; i < 6; i++)
            {
                Weapon weapon = new Weapon("Epée de feu", 10 * i, TypeEnum.FIRE, 15 * i, 7 * i);
                weapon.Level = i;
                Weapon hache = new Weapon("Hache de feu", 5 * i, TypeEnum.FIRE, 10 * i, 10 * i);
                hache.Level = i;
                Weapon dague = new Weapon("Dague de feu", 4 * i, TypeEnum.FIRE, 15 * i, 15 * i);
                dague.Level = i;
                weapons.Add(weapon);
                weapons.Add(hache);
                weapons.Add(dague);
            }
            for (int i = 1; i < 6; i++)
            {
                Weapon weapon = new Weapon("Epée d'eau", 10 * i, TypeEnum.WATER, 15 * i, 7 * i);
                weapon.Level = i;
                Weapon hache = new Weapon("Hache d'eau", 5 * i, TypeEnum.WATER, 10 * i, 10 * i);
                hache.Level = i;
                Weapon dague = new Weapon("Dague d'eau", 4 * i, TypeEnum.WATER, 15 * i, 15 * i);
                dague.Level = i;
                weapons.Add(weapon);
                weapons.Add(hache);
                weapons.Add(dague);
            }
            for (int i = 1; i < 6; i++)
            {
                Weapon weapon = new Weapon("Epée de vent", 10 * i, TypeEnum.WIND, 15 * i, 100);
                weapon.Level = i;
                Weapon hache = new Weapon("Hache de vent", 5 * i, TypeEnum.WIND, 10 * i, 10 * i);
                hache.Level = i;
                Weapon dague = new Weapon("Dague de vent", 4 * i, TypeEnum.WIND, 15 * i, 15 * i);
                dague.Level = i;
                weapons.Add(weapon);
                weapons.Add(hache);
                weapons.Add(dague);
            }
            for (int i = 1; i < 6; i++)
            {
                Weapon weapon = new Weapon("Epée de roche", 10 * i, TypeEnum.ROCK, 15 * i, 100);
                weapon.Level = i;
                Weapon hache = new Weapon("Hache de roche", 5 * i, TypeEnum.ROCK, 10 * i, 10 * i);
                hache.Level = i;
                Weapon dague = new Weapon("Dague de roche", 4 * i, TypeEnum.ROCK, 15 * i, 15 * i);
                dague.Level = i;
                weapons.Add(weapon);
                weapons.Add(hache);
                weapons.Add(dague);
            }
            return weapons;
        }

        public static List<Equipment> CreateEquipment()
        {
            List<Equipment> equipments = new List<Equipment>();
            for (int i = 1; i < 6; i++)
            {
                Equipment casque = new Equipment("Casque", 10 * i, TypeEnum.FIRE, 20 * i);
                casque.Level = i;
                equipments.Add(casque);
                Equipment gilet = new Equipment("Gilet", 10 * i, TypeEnum.FIRE, 20 * i);
                gilet.Level = i;
                equipments.Add(gilet);
                Equipment pantalon = new Equipment("Pantalon", 10 * i, TypeEnum.FIRE, 20 * i);
                pantalon.Level = i;
                equipments.Add(pantalon);
                Equipment shoes = new Equipment("Chaussure", 10 * i, TypeEnum.FIRE, 20 * i);
                shoes.Level = i;
                equipments.Add(shoes);
                Equipment shield = new Equipment("Bouclier", 10 * i, TypeEnum.FIRE, 20 * i);
                shield.Level = i;
                equipments.Add(shield);
            }
            return equipments;
        }
    }
}