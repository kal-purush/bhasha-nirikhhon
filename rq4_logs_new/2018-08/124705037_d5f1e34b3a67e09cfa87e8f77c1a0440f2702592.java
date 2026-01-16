import java.util.List;

public class Bishop extends Piece
{

	public Bishop(int colour, int type) 
	{
		super(colour, type);
	}
	
	@Override
	public List<Tile> getMoves()
	{
		for(int i = 1; getY() + i <= 8; i++)
		{			
			possY = getY() + i;
			possX = getX() + i;

			if(isLegal())
			{
				highlight();
			}
		}
		
		isTherePiece = false;

		for(int i = 1; getY() - i >= 0; i++)
		{
			possY = getY() - i;
			possX = getX() - i;
			
			if(isLegal())
			{
				highlight();
			}
		}
		
		isTherePiece = false;
		
		for(int i = 1; (getY() - i >= 0 && getX() + i <= 8); i++)
		{
			possY = getY() - i;
			possX = getX() + i;
			
			if(isLegal())
			{
				highlight();
			}
		}
		
		isTherePiece = false;
		
		for(int i = 1; (getY() + i <= 8 && getX() - i >= 0); i++)
		{
			possY = getY() + i;
			possX = getX() - i;
			
			if(isLegal())
			{
				highlight();
			}
		}
		
		isTherePiece = false;
		
		return tileOptions;
	}
}