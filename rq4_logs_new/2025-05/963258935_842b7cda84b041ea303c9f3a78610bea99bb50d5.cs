public class Player
{
    public List<string> Inventory { get; set; }
    public Room CurrentRoom { get; set; }
    public const string look = "look";
    public const string get = "get";
    public const string use = "use";
    public const string inv = "i";
    public const string help = "help";
    public const string north = "north";
    public const string south = "south";
    public const string east = "east";
    public const string west = "west";
    public const string exit = "exit";
    public const string quit = "quit";

    public Player(Room startRoom)
    {
        Inventory = new List<string>();
        CurrentRoom = startRoom;
    }

    public void Action(string playerAction, Player player)
    {

        if (CurrentRoom.Exits.ContainsKey(playerAction))
        {
            CurrentRoom = CurrentRoom.Exits[playerAction];
            Console.WriteLine($"You move {playerAction} and enter the {CurrentRoom.Name}.\n");
            Console.WriteLine(CurrentRoom.Description);

            //Call methods
            IterateItems(player);
            DisplayExits(CurrentRoom);

        }
        else if (playerAction == look)
        {
            Console.WriteLine($"{CurrentRoom.Name}\n");
            Console.WriteLine(CurrentRoom.Description);

            //Call methods
            IterateItems(player);
            DisplayExits(CurrentRoom);
        }
        else if (playerAction == inv)
        {
            //Call method
            ShowInventory(player);
        }
        else if (playerAction.Contains(get))
        {
            //Call method
            GetItem(playerAction, player);
        }
        else if (playerAction.Contains(use))
        {
            //Call method
            UseItem(playerAction, player);
        }
        else if (playerAction == help)
        {
            //show what kind of commands/actions the user can perform
            //Note: move this to it's own method
            Console.WriteLine("List of possible actions to perform: ");
            Console.WriteLine($"{look}, {inv}, {get}, {north}, {south}, {east}, {west} \n");
        }
        else
        {
            Console.WriteLine("You can't go that way.");
        }
    }

    static public void DisplayExits(Room CurrentRoom)
    {
        //check number of exits for CurrentRoom (the room the player is in)       
        string ExitsList = "";

        for (int i = 0; i < CurrentRoom.NumberOfExits.Count; i++)
        {
            if (CurrentRoom.NumberOfExits.Count <= 1)
            {
                ExitsList = "\n" + CurrentRoom.NumberOfExits[i];
            }
            else
            {
                ExitsList += "\n" + CurrentRoom.NumberOfExits[i];
            }
        }
        Console.WriteLine($"There are exit(s): {ExitsList}\n");
    }//End DisplayExits

    static public void IterateItems(Player player)
    {
        //Check if there are items in the room
        if (player.CurrentRoom.Items.Count > 0)
            Console.WriteLine("You see: ");

        //iterate items in room
        for (int i = 0; i < player.CurrentRoom.Items.Count(); i++)
        {
            Console.WriteLine(player.CurrentRoom.Items[i]);
        }

        //Add some space after item iteration
        Console.WriteLine();
    }//End of IterateItems

    static public void ShowInventory(Player player)
    {
        //Show player's inventory   
        //Check if player has any items
        if (player.Inventory.Count > 0)
            Console.WriteLine("You have: ");
        else
            Console.WriteLine("You have no items");

        for (int i = 0; i < player.Inventory.Count(); i++)
        {
            Console.WriteLine($"- {player.Inventory[i]}");
        }
        Console.WriteLine();
    }//End of ShowInventory

    static public void UseItem(string playerAction, Player player)
    {
        string item = "keycard";
        string room = "Bridge";

        if (player.Inventory.Contains(item) && player.CurrentRoom.Name == room)
        {
            Console.WriteLine($"You insert the {item} into the computer slot.");
        }
        else if (!player.Inventory.Contains(item))
            Console.WriteLine($"You don't have a {item}.");
    }
    static public void GetItem(string playerAction, Player player)
    {
        bool itemFound = false;
        bool missingItem = false;
        string tempItem = "";

        //Player can pickup any item in CurrentRoom's itemlist
        for (int i = 0; i < player.CurrentRoom.Items.Count; i++)
        {
            int itemIndex = 0;

            //Use String.Split method to split verb/items based on blank space (" ")
            string[] arrayWords = playerAction.Split(" ");

            //Split player input based on using blank space(" ") as a marker
            //Note: This means that currently this routine only accepts 1 blank space, i.e. "old newspaper" will fail
            //
            //Check if blank space (" ") can actually be found in the input string
            //or else we would get an ArgumentOutOfRangeException when assigning itemIndex
            if (playerAction.IndexOf(" ")! > -1)
                itemIndex = playerAction.IndexOf(" ");
            else
            {
                //inform user if input is missing a string (or item in this case). e.g. 'get card'/get keycard
                Console.WriteLine("What do you want to get?");
                missingItem = true;
                break;
            }

            //remove any whitespace
            tempItem = playerAction.Substring(itemIndex).Trim();

            //Extract the word from index 0 to before the blank space, in this case it's 'get'.
            //We don't currently use it but might later on
            //string get = playerAction.Substring(0, itemIndex);

            //if player input word is the same as an item in CurrentRoom
            //we add this item to inventory and remove it from CurrentRoom's itemlist
            if (tempItem.ToLower() == player.CurrentRoom.Items[i].ToLower())
            {
                player.Inventory.Add(player.CurrentRoom.Items[i]);

                //Remove item from CurrentRoom's itemlist
                player.CurrentRoom.Items.RemoveAt(i);

                Console.WriteLine($"You pick up {tempItem}.\n");
                itemFound = true;
                break;
            }
        }

        //Inform user if item is not in CurrentRoom's itemlist
        if (!itemFound && missingItem == false)
            Console.WriteLine($"There is no '{tempItem}' to pick up!\n");
    }//End of GetItem
}//End of class Player