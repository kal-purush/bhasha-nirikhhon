
public class ProgramUI
{
    private Character Player;

    public ProgramUI()
    {
        
    }

    public void RunGame() {
        Console.Clear();
        GameIntro();
        Player = new Human();
        Goblin goblin = new Goblin("Boblin the Goblin");
        CombatEncounter combat = new CombatEncounter(Player, goblin);
        Room room = new Room("a room made of mossy old stone, clearly once someone's home.\nThe floor is riddled with bones, waste, and rotten food.\nThe air is humid and putrid, moving heavily through your nostrils.\n\nFrom the shadows out of sight, you hear a guteral scream as a monster lunges forward at you!", combat);
        System.Console.WriteLine("\nYou ask the inn keeper for directions as you happily oblige, before setting off down the road to where the monsters are reported to be.\n");
        KeyPressPause();
        Console.Clear();
        room.ReadDescription();
        KeyPressPause();
        room.Encounter.StartEncounter();
        KeyPressPause();

        if (room.Encounter.Succeeded) {
            GameEndingSuccess();
        }
        KeyPressPause();
        RollCredits();

    }

    public static void GameIntro() {
        System.Console.WriteLine("Your weary feet lead you into a small farm town nestled in the woods.\nSeeking a warm place to rest, you head into an inn.\nThe inn keeper eyes your sword and excitedly greets you.\n");
        KeyPressPause();
        System.Console.WriteLine("Inn Keeper: Hello adventurer!\nWelcome to the town of Sorrowsoot. We are glad you came, we have been struggling with some monsters nearby!\nYou can stay the night for free if you rid us of them!\n");
        KeyPressPause();
        System.Console.WriteLine("Ever low on cash, you agree to help the town in exchange for a good night's sleep...\n");
    }
    public static void GameEndingSuccess() {
        Console.Clear();
        System.Console.WriteLine("You return, bloody and bruised back to the inn.\n\"The job is done. Give me a room, a beer, and a sandwich.\", you say to the inn keeper");
        System.Console.WriteLine("\"Gladly!\" the inn keeper says and you proceed to have a well earned night's dinner and a rest.");
    }

    public static void RollCredits() {
        Console.Clear();
        System.Console.WriteLine("\n\n                    The End\n\n");
        System.Console.WriteLine("                  Dungeon Crawl\n");
        System.Console.WriteLine("A C# group project by Roderick Walker and Jordan Hershberger\n\n\n\n\n\n\n");
    }
    public static void KeyPressPause() {
        System.Console.WriteLine("(press any key to continue)");
        Console.ReadKey();
        Console.Clear();
    }
}