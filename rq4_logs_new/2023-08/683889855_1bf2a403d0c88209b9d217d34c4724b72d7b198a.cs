using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

internal class FightScene
{
    List<Monster> monsters = new List<Monster>();
    Random rand = new Random();
    int stageLevel;

    public FightScene()
    {
        stageLevel = 1;
        CreateMonster(15, 5, "송사리");
        CreateMonster(10, 9, "꺽지");
        CreateMonster(25, 8, "블루길");
        CreateMonster(40, 15, "농어");
        CreateMonster(70, 25, "참치");
    }

    public void CreateMonster(int hp, int atk, string name)
    {
        Monster monster = new Monster(hp, atk, name);
        monsters.Add(monster);
    }

    public void StartPhase()
    {
        int[] monsterIndex = new int[5];
        for (int i = 0; i < 5; i++)
        {
            if (stageLevel == 1)
            {
                monsterIndex[i] = rand.Next(0, 3);
            }
            else if (stageLevel == 2)
            {
                monsterIndex[i] = rand.Next(0, 4);
            }
            else if (stageLevel == 3)
            {
                monsterIndex[i] = rand.Next(0, 5);
            }
        }

        int key = GameManager.GM.SelectOption(2, true);

        if (key == 0)
        {
            Program.entrance.EntranceUI();
        }
        else if (key == 1)
        {
            PlayerPhase();
        }
    }

    public void PlayerPhase()
    {

    }

    public void MonsterPhase()
    {

    }

    public void Result()
    {

    }
}