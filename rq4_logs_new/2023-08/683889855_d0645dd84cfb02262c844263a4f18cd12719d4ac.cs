using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

internal class StageSelectScene
{
    public void StageSelect()
    {
        Console.Clear();

        Console.WriteLine("1. 세 마리의 생선과 전투를 합니다!");
        Console.WriteLine("2. 네 마리의 생선과 전투를 합니다!");
        Console.WriteLine("3. 다섯 마리의 생선과 전투를 합니다!");
        Console.WriteLine("");

        bool isSelect = false;
        Console.SetCursorPosition(0, 28);
        Console.Write("선택지를 입력해주세요.: ");
        while (isSelect == false)
        {
            string key = Console.ReadLine();

            if (key == "1")
            {
                Program.fightScene.stageLevel = 1;
                Program.fightScene.Restart();
                Program.fightScene.StartPhase();
                isSelect = true;
            }
            else if (key == "2")
            {
                Program.fightScene.stageLevel = 2;
                Program.fightScene.Restart();
                Program.fightScene.StartPhase();
                isSelect = true;
            }
            else if (key == "3")
            {
                Program.fightScene.stageLevel = 3;
                Program.fightScene.Restart();
                Program.fightScene.StartPhase();
                isSelect = true;
            }
            else
            {
                Console.SetCursorPosition(0, 28);
                Console.Write("                                               ");
                Console.SetCursorPosition(0, 28);
                Console.Write("올바른 값을 입력해주세요.: ");
            }
        }
    }
}