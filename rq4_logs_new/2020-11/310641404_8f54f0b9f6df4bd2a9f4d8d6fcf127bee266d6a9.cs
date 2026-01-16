using System;
public class Menu
{
    public static string menuChoice;

    public static void MenuChoice()
    {
        Console.WriteLine("C O N S O L E  G U E S T B O O K\n");

        Console.WriteLine("1. Write Message");
        Console.WriteLine("2. Delete Message");
        Console.WriteLine("\nX. Quit");

        menuChoice = Console.ReadLine();
    }



    public static bool ValidateChoice()
    {
        if (menuChoice.ToUpper() == "X" | menuChoice == "1" | menuChoice == "2")
        {
            return true;
        }
        Console.Clear();
        return false;
    }  
}