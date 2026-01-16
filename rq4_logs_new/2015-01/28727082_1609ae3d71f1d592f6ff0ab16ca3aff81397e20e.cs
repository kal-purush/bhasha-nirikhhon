using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BattlePrototype
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello Summoner!");
            /*
            Spell Fireball = new Spell("Fireball", 80, 40, 25, 35, effect.None);
            Spell Bizzard = new Spell("Bizzard", 65, 25, 10, 20, effect.Frozen);
            Spell EarthSpike = new Spell("Earth Spikes", 40, 15, 0, 15, effect.Tiredness);
            */



            //=========================================================// Dwarf
            Unit Dwarf_warriors = new Unit(1, 10, 18, 5, 7, 5, 12, 1, race.dwarf);
            Unit Dwarf_boarriders = new Unit(2, 10, 18, 4, 6, 12, 10, 1, race.dwarf);
            Unit Dwarf_crossbowmans = new Unit(1, 8, 16, 3, 6, 5, 10, 15, race.dwarf);
            //=========================================================// Human
            Unit Human_soldiers = new Unit(2, 4, 16, 3, 2, 6, 40, 1, race.human);
            Unit Human_knights = new Unit(2, 4, 17, 3, 5, 14, 25, 1, race.human);
            Unit Human_archers = new Unit(3, 2, 12, 3, 2, 6, 30, 20, race.human);
            //=========================================================// Elfs
            Unit Elf_windsinger = new Unit(6, 4, 13, 5, 4, 5, 14, 25, race.elf);
            Unit Elf_panters = new Unit(5, 5, 14, 4, 4, 18, 16, 1, race.elf);
            //=========================================================// Undeads
            Unit Skeleton = new Unit(0, 1, 11, 2, 2, 5, 100, 1, race.undead);
            Unit Zombie = new Unit(-1, 8, 13, 2, 2, 3, 35, 1, race.undead);


            //=========================================================//Geen tribes
            Unit Rock_Troll = new Unit(-2, 95, 18, 3, 13, 6, 3, 2, race.barbarian);
            Unit Goblin = new Unit(9, 2, 10, 3, 1, 8, 65, 10, race.barbarian);
            Unit Orc = new Unit(2, 10, 12, 6, 5, 6, 15, 1, race.barbarian);

            //=========================================================//The 9-th Legion
            Unit Imp = new Unit(3, 3, 13, 4, 3, 9, 33, 1, race.daemon);
            Unit Crusader = new Unit(3, 19, 13, 6, 3, 13, 13, 1, race.daemon);



            //=========================================================

            Squad dwarf1 = new Squad(Dwarf_warriors, Dwarf_warriors.MaxAmount, "Dwarwen Warriors", true);
            Squad dwarf2 = new Squad(Dwarf_boarriders, Dwarf_boarriders.MaxAmount, "Dwarwen boarriders", true);
            Squad dwarf3 = new Squad(Dwarf_crossbowmans, Dwarf_crossbowmans.MaxAmount, "Dwarwen Crossbowgayz", true);

            Squad elf2 = new Squad(Elf_panters, Elf_panters.MaxAmount, "Night Sabel", true);
            Squad elf1 = new Squad(Elf_windsinger, Elf_windsinger.MaxAmount, "Windsingers", true);

            Squad human1 = new Squad(Human_soldiers, Human_soldiers.MaxAmount, "Empire soldiers", true);
            Squad human2 = new Squad(Human_archers, Human_archers.MaxAmount, "Archers", true);
            Squad human3 = new Squad(Human_knights, Human_knights.MaxAmount, "Ojo White Knights", true);


            Squad tribes1 = new Squad(Goblin, Goblin.MaxAmount, "Goblin Blowguners", false);
            Squad tribes2 = new Squad(Orc, Orc.MaxAmount, "Orcs Frenzy", false);
            Squad tribes3 = new Squad(Rock_Troll, Rock_Troll.MaxAmount, "Trolls Crusher", false);

            Squad undead1 = new Squad(Skeleton, Skeleton.MaxAmount, "Bones terror", false);
            Squad undead2 = new Squad(Zombie, Zombie.MaxAmount, "Hungry Ghouls", false);

            Squad daemon1 = new Squad(Imp, Imp.MaxAmount, "Flaming Imps", false);
            Squad daemon2 = new Squad(Crusader, Crusader.MaxAmount, "Winged crusader", false);

            List<Squad> squads1 = new List<Squad>();
            List<Squad> squads2 = new List<Squad>();
            /* squads1.Add(dwarf1);
             squads1.Add(dwarf2);
             squads1.Add(dwarf3);
            */
            squads1.Add(elf1);
            /*squads1.Add(elf2);
            
            squads1.Add(human1);
            squads1.Add(human2);
            squads1.Add(human3);
               */
            squads2.Add(tribes1);
            squads2.Add(tribes2);
            squads2.Add(tribes3);

            squads2.Add(undead1);
            squads2.Add(undead2);

            squads2.Add(daemon1);
            squads2.Add(daemon2);


            Console.BufferHeight = 1000;
            sandbox battelfield = new sandbox();

            battelfield.StartDuelSimulation(squads1, squads2, 1000);//simulate 2xN 1vs1 duels between every squad in list A and squads in  list B

            //squads1.OrderBy(s => s.initiative);

            Console.ReadLine();
        }

    }
}