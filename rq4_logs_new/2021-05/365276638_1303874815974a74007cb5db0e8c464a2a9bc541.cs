namespace Legacy
{
    public static class LegacyLoader
    {
        public static int Load(string path, out Area[] areas, out string character)
        {
            if (!File.Exists(path))
            {
                areas = null;
                return 2;
            }

            string[] file = File.ReadAllLines(path);
            Area[] newAreas = new Area[38];
            newAreas[0] = new Area("Intro_Solo");
            newAreas[1] = new Area("Intro_Team");
            newAreas[2] = new Area("Intro2_Team");
            newAreas[3] = new Area("Intro3_Team");
            newAreas[4] = new Area("Obelisk_Solo");
            newAreas[5] = new Area("Obelisk_Team");
            newAreas[6] = new Area("Obelisk2_Team");
            newAreas[7] = new Area("Spitblight_Solo");
            newAreas[8] = new Area("Spitblight_Team");
            newAreas[9] = new Area("Spitblight2_Team");
            newAreas[10] = new Area("Key1_Solo");
            newAreas[11] = new Area("Key1_Team");
            newAreas[12] = new Area("Turret_Solo");
            newAreas[13] = new Area("Turret_Team");
            newAreas[14] = new Area("Turret2_Team");
            newAreas[15] = new Area("Water_Solo");
            newAreas[16] = new Area("Water_Team");
            newAreas[17] = new Area("Treasure_Solo");
            newAreas[18] = new Area("Treasure_Team");
            newAreas[19] = new Area("Key2_Solo");
            newAreas[20] = new Area("Key2_Team");
            newAreas[21] = new Area("Switches_Solo");
            newAreas[22] = new Area("Switches_Team");
            newAreas[23] = new Area("Switches2_Team");
            newAreas[24] = new Area("Bottom_Solo");
            newAreas[25] = new Area("Bottom_Team");
            newAreas[26] = new Area("Bottom2_Team");
            newAreas[27] = new Area("SawPuzzle_Solo");
            newAreas[28] = new Area("SawPuzzle_Team");
            newAreas[29] = new Area("SawPuzzle2_Team");
            newAreas[30] = new Area("SawTrap_Solo");
            newAreas[31] = new Area("SawTrap_Team");
            newAreas[32] = new Area("FinalIntro_Solo");
            newAreas[33] = new Area("FinalIntro_Team");
            newAreas[34] = new Area("FinalIntro2_Team");
            newAreas[35] = new Area("FinalIntro3_Team");
            newAreas[36] = new Area("FinalShield_Solo");
            newAreas[37] = new Area("FinalShield_Team");

            foreach (Area area in newAreas)
            {
                area.Dialogues.Clear();
            }

            try
            {
                for (int i = 0; i < file.Length; i++)
                {
                    if (file[i].Contains("[DIALOGUE]"))
                    {
                        LoadData(file, i, newAreas);
                    }
                }
            }
            catch
            {
                areas = null;
                return 1;
            }

            areas = newAreas;
            return 0;
        }

        private static void LoadData(string[] file, int index, Area[] areas)
        {
            int area = int.Parse(file[index + 1].Substring(5));
            string portrait = file[index + 2].Substring(9);
            string content = file[index + 3].Substring(8);
            decimal start = decimal.Parse(file[index + 4].Substring(6));
            decimal end = decimal.Parse(file[index + 5].Substring(4));

            areas[area].Dialogues.Add(new Area.Dialogue { Portrait = portrait, Content = content, Start = start, End = end });
        }
    }
}