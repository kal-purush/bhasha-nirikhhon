
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
public class GameControl{
	MAIN main;
	int gameArray[][] = new int[MAIN.gameX][MAIN.gameY];
	int clickCount = 1;
	public GameControl(MAIN m){
		main = m;
		setDefaultGameArray();

	}
	public void userClick(int i, int j){
		if(!isClicked(i, j))
		{
			clickCount++;
			setGameArray(i, j, clickCount%2);
			main.gamePanel.setPermanentIcon(i, j, clickCount%2);
			main.gamePanel.setRolloverIcon((clickCount+1)%2);
			main.labelPanel.setText(clickCount%2);

			if(clickCount>5){
				if(isWin(i, j, (clickCount%2)+1)!=0){
					main.score[(clickCount)%2]++;
					JOptionPane.showMessageDialog(main, String.format("%s Won the Game", MAIN.playerName[(clickCount+1)%2]));
					main.scoreBoard.showScoreBoard();
					main.replay();
				}
				else if(clickCount==(MAIN.gameX*MAIN.gameY)+1)
				{
					JOptionPane.showMessageDialog(main, "Draw!!!");
					main.score[2]++;
					main.scoreBoard.showScoreBoard();
					main.replay();
				}

			}


		}
	}
	public void setGameArray(int i, int j, int player){

		gameArray[i][j]=player+1;
	}
	public boolean isClicked(int i, int j){
		if(gameArray[i][j]!=0)
			return true;
		return false;
	}
	public int isWin(int i, int j, int flag)
	{
		if(diagonalOne(i, j, flag)||diagonalTwo(i, j, flag)||leftRight(i, j, flag)||bottomUp(i, j, flag))
			return flag;
		return 0;

	}
	public void setDefaultGameArray()
	{
		for(int i=0; i<MAIN.gameX; i++)
			for(int j=0; j<MAIN.gameY; j++)
				gameArray[i][j]=0;
	}


	private boolean diagonalOne(int i, int j, int flag){
		int count=0;
		while(i>0&&j>0){
			if(gameArray[--i][--j]==flag)
				continue;
			else 
			{
				i++;
				j++;
				break;
			}
		}
		while(i<MAIN.gameX&&j<MAIN.gameY){
			if(gameArray[i++][j++]==flag)
				count++;
			else
				break;
		}
		if(count==MAIN.gameConstant)
			return true;
		return false;
	}

	private boolean diagonalTwo(int i, int j, int flag){
		int count=0;
		while(i>0&&j<MAIN.gameY-1){
			if(gameArray[--i][++j]==flag)
				continue;
			else
			{
				i++;
				j--;
				break;
			}
		}
		while(i<MAIN.gameX&&j>=0){
			if(gameArray[i++][j--]==flag)
				count++;
			else
				break;
		}
		if(count==MAIN.gameConstant)
			return true;
		return false;
	}
	private boolean bottomUp(int i, int j, int flag){
		int count=0;
		while(i>0){
			if(gameArray[--i][j]==flag)
				continue;
			else 
			{
				i++;
				break;
			}
		}
		while(i<MAIN.gameX){
			if(gameArray[i++][j]==flag)
				count++;
			else
				break;
		}
		if(count==MAIN.gameConstant)
			return true;
		return false;
	}
	private boolean leftRight(int i, int j, int flag){
		int count=0;
		while(j>0){
			if(gameArray[i][--j]==flag)
				continue;
			else 
			{
				j++;
				break;
			}
		}
		while(j<MAIN.gameY){
			if(gameArray[i][j++]==flag)
				count++;
			else
				break;
		}
		if(count==MAIN.gameConstant)
			return true;
		return false;
	}
}