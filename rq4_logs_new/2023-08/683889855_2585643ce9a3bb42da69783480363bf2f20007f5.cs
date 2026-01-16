using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

internal class BossScene
{
    Monster boss = new Monster(5);
    TheOldManAndTheSea theOldManAndTheSea = new TheOldManAndTheSea();
    LookAtThisCan lookAtThisCan = new LookAtThisCan();
    TunaSliced tunaSliced = new TunaSliced();
    Itadakimasu itadakimasu = new Itadakimasu();

    public void StartPhase()
    {
        Program.player.vsBoss = true;
        boss.Hp = 500;
        Console.Clear();
        DrawTuna(0, 0);
        DrawHpBar(0, 20);
        DrawInfoUI(53, 0);
        DrawStatUI(53, 10);
        DrawSkillUI(81, 10);
        Console.SetCursorPosition(55, 2);
        Console.Write("자이언트 첨치가 출현했습니다!");
        Console.ForegroundColor = ConsoleColor.Red;
        Console.SetCursorPosition(55, 3);
        Console.Write("녀석은 무지하게 화가 나 있습니다.");
        Console.ResetColor();
        Choice1();
    }

    public void SkillPhase()
    {
        ClearInfo();
        DrawStatUI(53, 10);
        Console.SetCursorPosition(55, 2);
        Console.Write("당신은 스킬 사용을 선택했습니다.");
        Console.SetCursorPosition(55, 3);
        Console.Write("사용할 스킬을 선택해주세요.");
        Console.SetCursorPosition(55, 4);
        Console.Write($"[{theOldManAndTheSea.Name}]{theOldManAndTheSea.Description}");
        Console.SetCursorPosition(55, 5);
        Console.Write($"[{lookAtThisCan.Name}]{lookAtThisCan.Description}");
        Console.SetCursorPosition(55, 6);
        Console.Write($"[{tunaSliced.Name}]{tunaSliced.Description}");
        Console.SetCursorPosition(55, 7);
        Console.Write($"[{itadakimasu.Name}]{itadakimasu.Description}");
        Choice2();
    }

    public void DamagePhase()
    {
        ClearInfo();
        DrawStatUI(53, 10);
        ReduceHpBar(0, 20);
        DrawSkillUI(81, 10);
        if (boss.Hp > 0)
        {
            Console.SetCursorPosition(55, 2);
            Console.Write("스킬 사용에 성공했습니다!");
            Console.ForegroundColor = ConsoleColor.Red;
            Console.SetCursorPosition(55, 3);
            Console.Write($"자이언트 참치의 체력은 {boss.Hp}남았습니다.");
            Console.ResetColor();
            Console.SetCursorPosition(55, 4);
            Console.Write("자이언트 참치는 몹시 화가 났습니다.");
            Console.SetCursorPosition(55, 5);
            Console.Write("다음 행동을 선택해주세요.");
            Choice3();
        }
        else
        {
            Program.player.vsBoss = false;
            Console.SetCursorPosition(55, 2);
            Console.Write("스킬 사용에 성공했습니다!");
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.SetCursorPosition(55, 3);
            Console.Write($"당신은 자이언트 참치를 쓰러뜨렸습니다!");
            Console.ResetColor();
            Choice6();
        }
    }

    public void MonsterPhase()
    {
        Random rand = new Random();
        int damage = rand.Next(20, 61);
        Program.player.Hp -= damage;

        ClearInfo();
        DrawStatUI(53, 10);

        if (Program.player.Hp > 0)
        {
            Console.SetCursorPosition(55, 2);
            Console.Write("자이언트 참치가 당신을 공격했습니다!");
            Console.ForegroundColor = ConsoleColor.Red;
            Console.SetCursorPosition(55, 3);
            Console.Write($"당신의 체력은 {Program.player.Hp}남았습니다.");
            Console.ResetColor();
            Console.SetCursorPosition(55, 4);
            Console.Write("다음 행동을 선택해주세요.");
            Choice4();
        }
        else
        {
            Program.player.vsBoss = false;
            Console.SetCursorPosition(55, 2);
            Console.Write("자이언트 참치가 당신을 공격했습니다!");
            Console.ForegroundColor = ConsoleColor.Red;
            Console.SetCursorPosition(55, 3);
            Console.Write($"당신은 더 이상 참치를 상대할 체력이 없습니다.");
            Console.ResetColor();
            Console.SetCursorPosition(55, 4);
            Console.Write("참치는 유유히 당신의 시야에서 사라졌습니다..");
            Choice5();
        }
    }

    public void Result()
    {
        ClearInfo();
        DrawStatUI(53, 10);

        Random rand = new Random();
        int gold = rand.Next(2000, 5001);

        Program.player.Gold += gold;
        Program.player.Exp += boss.Exp;

        Console.ForegroundColor = ConsoleColor.Blue;
        Console.SetCursorPosition(55, 2);
        Console.Write($"당신은 {boss.Exp}의 경험치를 얻었습니다!");
        Console.SetCursorPosition(55, 3);
        Console.Write($"당신은 {gold}의 골드를 얻었습니다!");
        Console.ResetColor();
        Choice5();
    }

    public void Choice1()
    {
        bool isSelect = false;
        Console.SetCursorPosition(53, 28);
        Console.Write("                                                                           ");
        Console.SetCursorPosition(53, 28);
        Console.Write("[0]도망가기 [1]스킬사용 | 선택지를 입력해주세요.: ");

        while (isSelect == false)
        {
            while (isSelect == false)
            {
                string key = Console.ReadLine();

                if (key == "0")
                {
                    Program.entrance.EntranceUI();
                    isSelect = true;
                }
                else if (key == "1")
                {
                    SkillPhase();
                    isSelect = true;
                }
                else
                {
                    Console.SetCursorPosition(53, 28);
                    Console.Write("                                                                           ");
                    Console.SetCursorPosition(53, 28);
                    Console.Write("[0]도망가기 [1]스킬사용 | 올바른 값을 입력해주세요.: ");
                }
            }
        }
    }

    public void Choice2()
    {
        bool isSelect = false;
        Console.SetCursorPosition(53, 28);
        Console.Write("                                                                           ");
        Console.SetCursorPosition(53, 28);
        Console.Write("[1]사용 [2]사용 [3]사용 [4]사용 | 선택지를 입력해주세요.: ");

        while (isSelect == false)
        {
            while (isSelect == false)
            {
                string key = Console.ReadLine();

                if (key == "1")
                {
                    theOldManAndTheSea.UseSkill(Program.player);
                    DamagePhase();
                    isSelect = true;
                }
                else if (key == "2")
                {
                    lookAtThisCan.UseSkill(Program.player, boss);
                    DamagePhase();
                    isSelect = true;
                }
                else if (key == "3")
                {
                    tunaSliced.UseSkill(Program.player, boss);
                    DamagePhase();
                    isSelect = true;
                }
                else if (key == "4")
                {
                    itadakimasu.UseSkill(Program.player, boss);
                    DamagePhase();
                    isSelect = true;
                }
                else
                {
                    Console.SetCursorPosition(53, 28);
                    Console.Write("                                                                           ");
                    Console.SetCursorPosition(53, 28);
                    Console.Write("[1]사용 [2]사용 [3]사용 [4]사용 | 올바른 값을 입력해주세요.: ");
                }
            }
        }
    }

    public void Choice3()
    {
        bool isSelect = false;
        Console.SetCursorPosition(53, 28);
        Console.Write("                                                                           ");
        Console.SetCursorPosition(53, 28);
        Console.Write("[0]도망가기 [1]계속하기 | 선택지를 입력해주세요.: ");

        while (isSelect ==  false)
        {
            while (isSelect == false)
            {
                string key = Console.ReadLine();

                if (key == "0")
                {
                    Program.entrance.EntranceUI();
                    isSelect = true;
                }
                else if (key == "1")
                {
                    MonsterPhase();
                    isSelect = true;
                }
                else
                {
                    Console.SetCursorPosition(53, 28);
                    Console.Write("                                                                           ");
                    Console.SetCursorPosition(53, 28);
                    Console.Write("[0]도망가기 [1]계속하기 | 올바른 값을 입력해주세요.: ");
                }
            }
        }
    }

    public void Choice4()
    {
        bool isSelect = false;
        Console.SetCursorPosition(53, 28);
        Console.Write("                                                                           ");
        Console.SetCursorPosition(53, 28);
        Console.Write("[0]도망가기 [1]스킬사용 | 선택지를 입력해주세요.: ");

        while (isSelect == false)
        {
            while (isSelect == false)
            {
                string key = Console.ReadLine();

                if (key == "0")
                {
                    Program.entrance.EntranceUI();
                    isSelect = true;
                }
                else if (key == "1")
                {
                    SkillPhase();
                    isSelect = true;
                }
                else
                {
                    Console.SetCursorPosition(53, 28);
                    Console.Write("                                                                           ");
                    Console.SetCursorPosition(53, 28);
                    Console.Write("[0]도망가기 [1]스킬사용 | 올바른 값을 입력해주세요.: ");
                }
            }
        }
    }

    public void Choice5()
    {
        bool isSelect = false;
        Console.SetCursorPosition(53, 28);
        Console.Write("                                                                           ");
        Console.SetCursorPosition(53, 28);
        Console.Write("[0]마을로 복귀하기 | 선택지를 입력해주세요.: ");

        while (isSelect == false)
        {
            while (isSelect == false)
            {
                string key = Console.ReadLine();

                if (key == "0")
                {
                    Program.entrance.EntranceUI();
                    isSelect = true;
                }
                else
                {
                    Console.SetCursorPosition(53, 28);
                    Console.Write("                                                                           ");
                    Console.SetCursorPosition(53, 28);
                    Console.Write("[0]마을로 복귀하기 | 올바른 값을 입력해주세요.: ");
                }
            }
        }
    }

    public void Choice6()
    {
        bool isSelect = false;
        Console.SetCursorPosition(53, 28);
        Console.Write("                                                                           ");
        Console.SetCursorPosition(53, 28);
        Console.Write("[0]보상 확인하기 | 선택지를 입력해주세요.: ");

        while (isSelect == false)
        {
            while (isSelect == false)
            {
                string key = Console.ReadLine();

                if (key == "0")
                {
                    Result();
                    isSelect = true;
                }
                else
                {
                    Console.SetCursorPosition(53, 28);
                    Console.Write("                                                                           ");
                    Console.SetCursorPosition(53, 28);
                    Console.Write("[0]보상 확인하기 | 올바른 값을 입력해주세요.: ");
                }
            }
        }
    }

    public void DrawInfoUI(int x, int y)
    {
        Console.SetCursorPosition(x, y);
        Console.Write("*---------------------- INFO ---------------------*");
        Console.SetCursorPosition(x, y + 8);
        Console.Write("*-------------------------------------------------*");
        for (int i = 0; i < 7; i++)
        {
            Console.SetCursorPosition(x, y + i + 1);
            Console.Write("|");
            Console.SetCursorPosition(x + 50, y + i + 1);
            Console.Write("|");
        }
    }

    public void ClearInfo()
    {
        Console.SetCursorPosition(54, 1);
        Console.Write("                                                 ");
        Console.SetCursorPosition(54, 2);
        Console.Write("                                                 ");
        Console.SetCursorPosition(54, 3);
        Console.Write("                                                 ");
        Console.SetCursorPosition(54, 4);
        Console.Write("                                                 ");
        Console.SetCursorPosition(54, 5);
        Console.Write("                                                 ");
        Console.SetCursorPosition(54, 6);
        Console.Write("                                                 ");
        Console.SetCursorPosition(54, 7);
        Console.Write("                                                 ");
    }

    public void DrawStatUI(int x, int y)
    {
        Console.SetCursorPosition(x, y);
        Console.Write("*--------- STAT ---------*");
        Console.SetCursorPosition(x, y + 8);
        Console.Write("*------------------------*");
        for (int i = 0; i < 7; i++)
        {
            Console.SetCursorPosition(x, y + i + 1);
            Console.Write("|");
            Console.SetCursorPosition(x + 25, y + i + 1);
            Console.Write("|");
        }

        Console.SetCursorPosition(x + 4, y + 2);
        Console.Write($"                     ");
        Console.SetCursorPosition(x + 4, y + 3);
        Console.Write($"                     ");
        Console.SetCursorPosition(x + 4, y + 4);
        Console.Write($"                     ");
        Console.SetCursorPosition(x + 4, y + 5);
        Console.Write($"                     ");
        Console.SetCursorPosition(x + 4, y + 6);
        Console.Write($"                     ");

        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.SetCursorPosition(x + 4, y + 2);
        Console.Write($"플레이어 레벨: {Program.player.Level}");
        Console.SetCursorPosition(x + 4, y + 3);
        Console.Write($"플레이어 체력: {Program.player.Hp}");
        Console.SetCursorPosition(x + 4, y + 4);
        Console.Write($"플레이어 마나: {Program.player.Mp}");
        Console.SetCursorPosition(x + 4, y + 5);
        Console.Write($"플레이어 공격: {Program.player.Atk}");
        Console.SetCursorPosition(x + 4, y + 6);
        Console.Write($"플레이어 방어: {Program.player.Def}");
        Console.ResetColor();
    }

    public void DrawSkillUI (int x, int y)
    {
        Console.SetCursorPosition(x, y);
        Console.Write("*------- SKILL -------*");
        Console.SetCursorPosition(x, y + 8);
        Console.Write("*---------------------*");
        for (int i = 0; i < 7; i++)
        {
            Console.SetCursorPosition(x, y + i + 1);
            Console.Write("|");
            Console.SetCursorPosition(x + 22, y + i + 1);
            Console.Write("|");
        }

        Console.SetCursorPosition(x + 2, y + 2);
        Console.Write($"[1] {theOldManAndTheSea.Name} ");
        Console.ForegroundColor = ConsoleColor.Blue;
        Console.Write($"/ {theOldManAndTheSea.RequiredMp}MP");
        Console.ResetColor();
        Console.SetCursorPosition(x + 2, y + 3);
        Console.Write($"[2] {lookAtThisCan.Name} ");
        Console.ForegroundColor = ConsoleColor.Blue;
        Console.Write($"/ {lookAtThisCan.RequiredMp}MP");
        Console.ResetColor();
        Console.SetCursorPosition(x + 2, y + 4);
        Console.Write($"[3] {tunaSliced.Name} ");
        Console.ForegroundColor = ConsoleColor.Blue;
        Console.Write($"/ {tunaSliced.RequiredMp}MP");
        Console.ResetColor();
        Console.SetCursorPosition(x + 2, y + 5);
        Console.Write($"[4] {itadakimasu.Name} ");
        Console.ForegroundColor = ConsoleColor.Blue;
        Console.Write($"/ {itadakimasu.RequiredMp}MP");
        Console.ResetColor();
    }

    public void DrawHpBar(int x, int y)
    {
        Console.SetCursorPosition(x + 1, y);
        Console.BackgroundColor = ConsoleColor.Red;
        for (int i = 0; i < 50; i++)
        {
            Console.Write(" ");
        }
        Console.ResetColor();
        Console.SetCursorPosition(x, y);
        Console.Write("|");
        Console.SetCursorPosition(x + 51, y);
        Console.Write("|");
    }

    public void ReduceHpBar(int x, int y)
    {
        if (boss.Hp < 500)
        {
            for (int i = 50; i > 0; i--)
            {
                if (boss.Hp <= i * 10)
                {
                    Console.SetCursorPosition(x + i, y);
                    Console.Write(" ");
                }
            }
        }
    }

    public void DrawTuna(int x, int y)
    {
        Console.SetCursorPosition(x, y + 0);
        Console.Write("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@~ @@@@@@@@@@@@@@@@@");
        Console.SetCursorPosition(x, y + 1);
        Console.Write("@@@@@@@@@@@@@@@@@@@**=@@@@@@@@@*~.:@@@@@@@@@@@@=@@@@");
        Console.SetCursorPosition(x, y + 2);
        Console.Write("@@@@@@@@@@@@@@@@@@!,,-;:@@@@@#;.:!@@@@@@@@@@@;;,@@@@");
        Console.SetCursorPosition(x, y + 3);
        Console.Write("@@@@@@@@@@@@@@@@@~,,,,,,-#@@#--,!@@@@@@@@@@@;..$@@@@");
        Console.SetCursorPosition(x, y + 4);
        Console.Write("@@@@@@@@@@@@@@@!.....-,, !:,~@@@@@@@@@@@@!  :@@@@@@@");
        Console.SetCursorPosition(x, y + 5);
        Console.Write("@@@@@@@@@@#$.                     =$@@@@@@!  $@@@@@@");
        Console.SetCursorPosition(x, y + 6);
        Console.Write("@@@@@@@@@*~*                         $#@@=   $@@@@@@");
        Console.SetCursorPosition(x, y + 7);
        Console.Write("@@@@@@@; ~~-:                        ,~:;,  ~#@@@@@@");
        Console.SetCursorPosition(x, y + 8);
        Console.Write("@@@@@@~.  :.,                               *@@@@@@@");
        Console.SetCursorPosition(x, y + 9);
        Console.Write("@@@@@@,          ~,,,,,,,,,,,,,,,*########* *@@@@@@@");
        Console.SetCursorPosition(x, y + 10);
        Console.Write("@@@@@@~=######;~~~~~~~~~~;##########$#=~@.   $@@@@@@");
        Console.SetCursorPosition(x, y + 11);
        Console.Write("@@@@@@@; !#######################=$!;!==@@!   $@@@@@");
        Console.SetCursorPosition(x, y + 12);
        Console.Write("@@@@@@@@!~*######$$$$$#######$:=;#**@@@@@@:  ~@@@@@@");
        Console.SetCursorPosition(x, y + 13);
        Console.Write("@@@@@@@@@$;=@####$$$$$#####@;~ ~#@@@@@@@@@@:  @@@@@@");
        Console.SetCursorPosition(x, y + 14);
        Console.Write("@@@@@@@@@@@@@ *##########= !:,~@@@@@@@@@@@@@~  @@@@@");
        Console.SetCursorPosition(x, y + 15);
        Console.Write("@@@@@@@@@@@@@@#$=*$;~~~=$#@@#-,,*@@@@@@@@@@@@#-,@@@@");
        Console.SetCursorPosition(x, y + 16);
        Console.Write("@@@@@@@@@@@@@@@@@!:!@@@@@@@@@@:::!@@@@@@@@@@@@@!@@@@");
        Console.SetCursorPosition(x, y + 17);
        Console.Write("@@@@@@@@@@@@@@@@@@*$@@@@@@@@@@@#*-:@@@@@@@@@@@@@@@@@");
        Console.SetCursorPosition(x, y + 18);
        Console.Write("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@#,@@@@@@@@@@@@@@@@@");

        Console.SetCursorPosition(x, y + 22);
        Console.Write("  ::::::::::   :::    :::    ::::    :::        ::: ");
        Console.SetCursorPosition(x, y + 23);
        Console.Write("     :+:      :+:    :+:    :+:+:   :+:      :+: :+:");
        Console.SetCursorPosition(x, y + 24);
        Console.Write("    +:+      +:+    +:+    :+:+:+  +:+     +:+   +:+");
        Console.SetCursorPosition(x, y + 25);
        Console.Write("   +#+      +#+    +:+    +#+ +:+ +#+    +#++:++#++:");
        Console.SetCursorPosition(x, y + 26);
        Console.Write("  +#+      +#+    +#+    +#+  +#+#+#    +#+     +#+ ");
        Console.SetCursorPosition(x, y + 27);
        Console.Write(" #+#      #+#    #+#    #+#   #+#+#    #+#     #+#  ");
        Console.SetCursorPosition(x, y + 28);
        Console.Write("###       ########     ###    ####    ###     ###   ");

        Console.ForegroundColor = ConsoleColor.Red;
        Console.SetCursorPosition(x + 56, y + 20);
        Console.Write(":::::::::    ::::::::    ::::::::    ::::::::  ");
        Console.SetCursorPosition(x + 56, y + 21);
        Console.Write(":+:    :+:  :+:    :+:  :+:    :+:  :+:    :+: ");
        Console.SetCursorPosition(x + 56, y + 22);
        Console.Write("+:+   +:+   +:+    +:+  +:+         +:+        ");
        Console.SetCursorPosition(x + 56, y + 23);
        Console.Write("+#++:++#+   +#+    +:+  +#++:++#++  +#++:++#++ ");
        Console.SetCursorPosition(x + 56, y + 24);
        Console.Write("+#+    +#+  +#+    +#+         +#+         +#+ ");
        Console.SetCursorPosition(x + 56, y + 25);
        Console.Write("#+#    #+#  #+#    #+#  #+#    #+#  #+#    #+# ");
        Console.SetCursorPosition(x + 56, y + 26);
        Console.Write("#########    ########    ########    ########  ");
        Console.SetCursorPosition(x + 56, y + 27);
        Console.ResetColor();
    }
}