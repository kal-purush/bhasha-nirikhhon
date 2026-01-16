using System;

public class BoardSpace : System.Windows.Forms.Button
{
    private int x;
    private int y;
    private int state;
    public BoardSpace()
	{
	}
    public BoardSpace(int xCoord, int yCoord, int inState)
    {
        x = xCoord;
        y = yCoord;
        state = inState;
    }
    public BoardSpace(int xCoord, int yCoord)
    {
        x = xCoord;
        y = yCoord;
        state = 0;
    }
    public int getX()
    {
        return x;
    }
    public int getY()
    {
        return y;
    }
    //getState() and setState() will be scrubbed when the merge is complete
    public int getState()
    {
        return state;
    }
    public void setState(int inState)
    {
        state = inState;
    }
}