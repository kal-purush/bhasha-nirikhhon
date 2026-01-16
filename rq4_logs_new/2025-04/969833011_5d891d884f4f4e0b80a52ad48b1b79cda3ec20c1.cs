namespace TextRPG_Team25.Core
{
    internal class JobMenu
    {
        private Player player;

        public JobMenu(Player player)
        {
            this.player = player;
        }

        public void ShowJobSelectionMenu()
        {
            while (true)
            {
                Console.Clear();
                Console.WriteLine("직업을 선택하세요:\n");
                Console.WriteLine("1. 검투사 - 강철의 심장을 지닌 근접 전사, 맨 앞에서 적을 짓밟는다.");
                Console.WriteLine("2. 화염술사 - 모든 것을 태워버리는 불꽃의 인도자, 열기로 적을 압도한다.");
                Console.WriteLine("3. 얼음술사 - 차가운 침묵 속에 적의 움직임을 봉인하는 냉기의 지배자.\n");
                Console.WriteLine("0. 나가기\n");
                Console.Write(">> ");

                string input = Console.ReadLine();

                switch (input)
                {
                    case "1":
                        player.job = "검투사";
                        Console.WriteLine("\n검투사를 선택하였습니다!");
                        Console.ReadKey();
                        return;
                    case "2":
                        player.job = "화염술사";
                        Console.WriteLine("\n화염술사를 선택하였습니다!");
                        Console.ReadKey();
                        return;
                    case "3":
                        player.job = "얼음술사";
                        Console.WriteLine("\n얼음술사를 선택하였습니다!");
                        Console.ReadKey();
                        return;
                    case "0":
                        return;
                    default:
                        Console.WriteLine("\n잘못된 입력입니다.");
                        Console.ReadKey();
                        break;
                }
            }
        }
    }
}