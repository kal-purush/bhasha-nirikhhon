using System;
using System.Collections.Generic;

namespace BlightnautsDialogue
{
    public static class ProjectManager
    {
        public readonly static string[] Characters = new string[]
        {
            "Assassin",
            "Bird",
            "Blazer",
            "Blazer_4",
            "Blazer_5",
            "Blinker",
            "Blinker_2",
            "Boizor",
            "Boizor_2",
            "Brute",
            "Butterfly",
            "Butterfly_4",
            "Captain",
            "Captain_2",
            "Chameleon",
            "Chameleon_3",
            "Chameleon_4",
            "Cowboy",
            "Cowboy_4",
            "Commando",
            "Commando_2",
            "Commando_3",
            "Commando_4",
            "Crawler",
            "Crawler_2",
            "Crumple",
            "Crumple_2",
            "Dasher",
            "Ellipto",
            "Ellipto_3",
            "Gantlet",
            "Gantlet_2",
            "Gladiator",
            "Gladiator_2",
            "Heavy",
            "Heavy_3",
            "Hunter",
            "Hunter_4",
            "Hyper",
            "Hyper_2",
            "Jetter",
            "Maw",
            "Maw_3",
            "Maw_4",
            "Paladin",
            "Paladin_2",
            "Paladin_4",
            "Poacher",
            "Poacher_2",
            "Rascal",
            "Rascal_2",
            "Shaman",
            "Shaman_2",
            "Shifter",
            "Shifter_2",
            "Spy",
            "Spy_2",
            "Spy_3",
            "Summoner",
            "Tank",
            "Tank_4",
            "Vampire",
            "Wakuwaku",
            "Wakuwaku_2",
            "Warrior",
            "Warrior_2",
            "Wozzle",
            "Wozzle_3"
        };

        private static string[] areasNames;
        public static string Path { get; private set; }
        public static List<Area> Areas { get; private set; }

        public static void LoadProject()
        {
            // No arguments for now.
            Areas = new List<Area>();
            Areas.Add(new Area("test_area"));
            Areas[0].GetCharacter("Blinker").SoloDialogue.Add(new Area.Dialogue
            (
                "ClementH doesn't like me :(",
                "IconCharacterBlinker",
                2,
                0
            ));
        }
    }
}