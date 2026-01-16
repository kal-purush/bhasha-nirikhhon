package Game;

public class BasePlayer
{
    private String name;
    private int wins;
    private int losses;

    public BasePlayer()
    {
        name = "Default Player";
        wins = 0;
        losses = 0;
    }

    public BasePlayer(String n)
    {
        name =  n;
        wins = 0;
        losses = 0;
    }

    public void increaseWins()
    {
        wins++;
    }

    public void increaseLosses()
    {
        losses--;
    }

    public void resetStats()
    {
        wins =0;
        losses=0;
    }

    public String getName()
    {
        return name;
    }

    public int getWins()
    {
        return wins;
    }

    public int getLosses()
    {
        return losses;
    }

}