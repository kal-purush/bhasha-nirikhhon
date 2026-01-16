
public class TicTacToeGame {

	static char array[] =new char[10];
	
	public static void main(String[] args) {
		TicTacToeGame.displayGame();
	}

static public void displayGame(){
	for(int i=0;i<10;i++){
		array[i]=Character.forDigit(0,10);
		System.out.println(array[i]);
	}
}
}