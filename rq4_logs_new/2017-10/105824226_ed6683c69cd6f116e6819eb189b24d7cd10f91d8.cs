using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.IO;

// Analysis disable once CheckNamespace
public class MoneyManagement : MonoBehaviour {
    EndGame cEndGame;
    List<string> lstEvent;
	List<int> lstPastGames;
    public int money{ get; private set; }

    int timesLostMoney;
    // Use this for initialization
    void Awake () {
        cEndGame = new EndGame();

        lstEvent = new List<string>();
        lstPastGames = new List<int>();
        money = 100000;
        timesLostMoney = 0;

        lstEvent.Add("You have water in the basement. Because this is a game, repairs will be instantanious, but it will cost you");
        lstEvent.Add("You saw a spider and it scared you. You lost precious wokring hours crying to your mom.");
        lstEvent.Add("A hobo dragged your ass into court over some bullshit. You won your case, but he'll never pay and your lawyors want their money");
        lstEvent.Add("There is no valid reason why you lost money. I just don't like you.");
        lstEvent.Add("Your house got broken into. Nothing was stolen, but the tresspassers took a shit on evert desk and table in the house");
        lstEvent.Add("Hi I'm a mac and I am fuc**ng expansive");
        lstEvent.Add("Who would have thaught that a zombie apocalypse could be so bad for your buisness. There are no actual zombies, but you still lost some money");
        lstEvent.Add("I want the new iPhone, and you are paying for it");
        lstEvent.Add("Trump said: I will build a wall and the player will pay for it");
        lstEvent.Add("A fireplace is nice. Especially when it burns your house down");
        lstEvent.Add("I forgot to factor inflation into your finances. I had to catch up for the past 15 years. Just to be clear, I did not take your money,  it just disapeared.");
        lstEvent.Add("You know what you did");
        lstEvent.Add("Idiots doing some stocks trading is like kids playing in the highway; it is fun to watch");
        lstEvent.Add("Your EX called you. After hours of her telling you how you ruined her life, he tells you she is calling on reverse billling from another continent");
        lstEvent.Add("You discovered a way to cure cancer, but big pharmaceuticals would lose a lot of money because of that. They broke your knees instead.");
        lstEvent.Add("Aparently vaccins cause autisme. Also you have HIV");
        lstEvent.Add("That freindly nigerian prince never ended up sending you that money");
        lstEvent.Add("You know that coworker's bean bag you just broke? Turns out it was stuffed with cocaine... and now you have to buy him another one.");
        lstEvent.Add("Keeping a fire extinguisher around is good for your safety in case of a fire… except when it is filled with jet fuel.");
        lstEvent.Add("Your dog decided it is now his house. you can go and buy another one");

        /* the past games list is a decoy
         * remove once database is implemented
         */

        lstPastGames.Add(13);
        lstPastGames.Add(15);
        lstPastGames.Add(2);
        lstPastGames.Add(21);

    }
	
	// Update is called once per frame
	void Update () {
		
	}

    void moneyLoss(int loss)
    {
        if (CheckEndGame(loss) == false)
        {
            timesLostMoney++;
            money -= loss;
        }

    }

    void moneyGain(int gain)
    {
        money += gain;
    }

    

    public string RandomEvents()
    {
        int lowerMargin = 1000;
        int higherMargin = 10000;
        int lostMoney = Random.Range(lowerMargin, higherMargin);
        //random money loss displayed here

        Debug.Log("lstEvent.Lengh " + lstEvent.Count);
        int index = Random.Range(0, lstEvent.Count - 1);
        Debug.Log(index);
        return lstEvent[index];
    }

    public bool CheckEndGame(int loss)
    {
        /* checks if money gets into the negative. 
         * if it is the case, return true else return false
         */

        if (loss > money)
        {
            cEndGame.EndGameFunction( timesLostMoney, lstPastGames);
            return true;
        }
        else
        {
            return false;
        }
    }
}

// Analysis disable once CheckNamespace
public class EndGame
{

 

    public void EndGameFunction(int daysSurvived, List<int> lstScores)
    {
        int bestScore = findHighScore(lstScores);
        Debug.Log("You survived " + daysSurvived + " the previous highscore was " + bestScore);
        /*end game here*/

    }

    int findHighScore(List<int> lstScores)
    {
        int lenght = lstScores.Count;
        int highscore = 0;
        for (int i = 0; i < lenght; i++)
        {
            if (highscore < lstScores[i])
                highscore = lstScores[i];
        }
        return highscore;

    }
}
