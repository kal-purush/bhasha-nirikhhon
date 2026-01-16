class Entrance
{
    public Entrance()
    {
    }

    public void EntranceUI()
    {
        int optionNum = 2;
        Console.WriteLine("참치 사냥을 떠나는 것이에요\n");

        Console.WriteLine("1. 상태보기 2. 던전 입장");
        int input = GameManager.GM.SelectOption(optionNum, false,"");
        switch (input)
        {
            case 1:
                Console.WriteLine("상태 보기");
                break;
            case 2:
                Console.WriteLine("던전 입장");
                break;
        }
    }
}