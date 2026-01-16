
package skunk.domain;

//import edu.princeton.cs.introcs.StdOut;

//Roll: Throw Dice ( pair of die ) once and save the sum of values.

public class Roll
{
	private Dice dice;
	private int iRollTotal;
	private SkunkUI ui1;
	
	//**********************************************************
	
	public Roll()
	{
		ui1 = new SkunkUI();
		//ui1.printLine( "In Roll constructor: ");
		dice = new Dice();
	}

	//**********************************************************
	
	public void rollDice() 
	{
		dice.roll();
		ui1.printLine( dice.toString() );
		return;
	}
		
	//**********************************************************
	
	public int getLastRollValue() 
	{
		int iValue = dice.getLastRoll();
		dice.toString();
		
		set_iRollTotal( iValue);
		return iValue;
	}
	
	//**********************************************************
	
	public int get_iRollTotal() 
	{
		return iRollTotal;
	}

	public void set_iRollTotal(int iRollTotal) 
	{
		this.iRollTotal = iRollTotal;
	}
	
	//*********************roll*************************************
	
	public boolean isRegularSkunk()
	{
		int iDie1 = dice.getPointerDie1().getLastRoll();
		int iDie2 = dice.getPointerDie2().getLastRoll();
		
		//if( ( dice.getPointerDie1().getLastRoll() == 1 ) ||
		//		( dice.getPointerDie2().getLastRoll() == 1 ) )
		if( iDie1 == 1 && iDie2 == 1)
		{
			//ui1.printLine( "isRegularSkunk: false1 " + iDie1 + " " + iDie2);
			return false;
		}
		else if( iDie1 == 1 || iDie2 == 1)
		{
			//ui1.printLine( "isRegularSkunk: true "+ iDie1 + " " + iDie2);
			return true;
		}
		else
		{
			//ui1.printLine( "isRegularSkunk: false2 "+ iDie1 + " " + iDie2);
			return false;
		}
		
	}
	
	//**********************************************************
	
	public boolean isDoubleSkunk()
	{
		if( getLastRollValue() == 2 )
			return true;
		else
			return false;
	}
	
	//**********************************************************
	
	public boolean isSkunkDeuce()
	{
		if( getLastRollValue() == 3 )
			return true;
		else
			return false;
	}
}