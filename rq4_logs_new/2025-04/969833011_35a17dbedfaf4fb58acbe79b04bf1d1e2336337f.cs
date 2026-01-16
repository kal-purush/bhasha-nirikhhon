namespace TextRPG_Team25.UI
{
    internal class Utils
    {
        // 글자 색 바꾸기 ex) Utils.ColoredText("메인 메뉴", ConsoleColor.DarkRed);
        public static void ColoredText(string text, ConsoleColor color)
        {
            Console.ForegroundColor = color;
            Console.Write(text);
            Console.ResetColor();
        }

        // 입력 메뉴 스타일 ex) Utils.MenuOption("0", "게임 종료");
        public static void MenuOption(string number, string label, ConsoleColor color = ConsoleColor.Yellow)
        {
            Console.ForegroundColor = color;
            Console.Write($"[{number}] ");
            Console.ResetColor();
            Console.WriteLine(label);
        }

        // 한 글자씩 출력
        public static void TypeEffect(string text, ConsoleColor color = ConsoleColor.White, int delay = 50)
        {
            foreach (char c in text)
            {
                Console.Write(c);
                Thread.Sleep(delay);
            }
            Console.WriteLine();
            Console.ReadKey();
            Console.ResetColor();
        }
    }
}