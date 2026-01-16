using UnityEngine;
using System.Collections;

public class session_monitor : MonoBehaviour {
    // Used to monitor the players condition
    public int cash = 100;
    public int days_to_live = 30;
    public uint cash_goal = 100100;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called on think
	void FixedUpdate () {
	    // CLOCK HERE
	}
    
    public bool dayEnd_CheckMoney()
    {
        // Make sure they dont end the day in debt
        if (this.cash < 1)
        {
            // Die
            this.killPlayer();
        }else if (this.cash * 1.06 >= this.cash_goal)
        {

        }
        return true;
    }

    public void addCash(int amt)
    {
        this.cash += amt;
    }

    public bool atGoal()
    {
        return (this.cash >= this.cash_goal);
    }

    public int getCash()
    {
        return this.cash;
    }
    
    public void setCash(int amt)
    {
        this.cash = amt;

        if (this.cash >= this.cash_goal)
        {
            // You win
            this.freePlayer();
        }
    }



    public int minusDay()
    {
        this.days_to_live = Mathf.Max(this.days_to_live - 1, 0);

        // Check if they need to be taken out back
        // And hit really hard with a 9 iron
        // in the kneecaps lol

        if (this.days_to_live == 0)
        {
            this.killPlayer();
            return 0;
        }

        return this.days_to_live;
    }

    public void freePlayer()
    {
        // Looks like you actually
        // beat the odds

        Debug.Log("Congradulations, you have beaten the odds!");
        this.endGame();
    }

    public void killPlayer()
    {
        // Knee cap this smuck!
       

        //@@@ END GAME CODE HERE
        Debug.Log("GAME END CODE HERE!!! FIX ME!!");
        this.endGame();
    }
    private void endGame()
    {
        //@@@ Your code here
        Application.Quit();
    }
}