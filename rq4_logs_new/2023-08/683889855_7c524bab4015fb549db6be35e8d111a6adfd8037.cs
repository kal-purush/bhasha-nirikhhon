class Inventory
{
	public void DisplayInventory()
	{
        Console.Clear();
        Console.WriteLine("[인벤토리]");
		Console.WriteLine();
		Console.WriteLine("1. 장비아이템"); //장비
		Console.WriteLine("2. 소비아이템"); //물약 
        Console.WriteLine();
        Console.WriteLine("0. 뒤로가기");

        int inputKey = GameManager.GM.SelectOption(3, false);
        switch (inputKey)
        {

            case 1:
                return;
            case 2:
                return;
            case 0:
                return;
            default:
                break;
        }
    }
}

