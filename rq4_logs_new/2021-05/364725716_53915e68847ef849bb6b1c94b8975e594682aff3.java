import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.Scanner;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.layout.BorderPane;
import javafx.scene.paint.Paint;
import javafx.stage.Stage;

public class TicTacClient extends Application{
	
	private final int PORT = 8888;
	private final int BOX_SIZE = 100;
	private final int ROW = 3;
	private final int COL = 3;
	private GraphicsContext gc;
	private Scanner in;
	private PrintWriter out;
	private String player;
	private int[][] board;
	

	
	public static void main(String[] args) throws IOException {
		

		Application.launch(args);
		
	}

	@Override
	public void start(Stage stage) throws Exception {
		
		
		BorderPane pane = new BorderPane();
		
		Canvas canvas = new Canvas(300, 300);
		gc = canvas.getGraphicsContext2D();
		
		pane.setCenter(canvas);
		
		//Draw Board
		gc.strokeLine(BOX_SIZE, 0, BOX_SIZE, 300);
		gc.strokeLine(BOX_SIZE + 100, 0, BOX_SIZE + 100, 300);
		gc.strokeLine(0, BOX_SIZE, 300, BOX_SIZE);
		gc.strokeLine(0, BOX_SIZE + 100, 300,  BOX_SIZE + 100);
		
		
		stage.setScene(new Scene(pane, 300, 300));
		stage.setTitle("TicTacToe");
		
		stage.show();

		new Thread() {
			public void run() {
				playGame();
			}
		}.start();
		
	}
	private void playGame() {

		String msg;
		try {

			
			Socket s = new Socket("localhost", PORT);

			Scanner in = new Scanner(s.getInputStream());
			out = new PrintWriter(s.getOutputStream());

			//set board
			board = new int[ROW][COL];
			for(int i = 0; i < ROW; i++) {
				for(int j = 0; j < COL; j++) {
					board[i][j] = -1;
				}
			}
			
			
			out.println("Hello");
			out.flush();
			msg = in.nextLine();	
			System.out.println(msg);

			player = msg.replaceAll("[^0-9]+", "");
			
			
			
			boolean moveAgain = false;
			
			int counter = 0;
			
			do {
				//first player
				makeMove();
				msg = in.nextLine();//your move back first
				
				if(msg.matches("Player [1-2] has chosen [0-2] [0-2]")) {
					
					
					String[] tokens = msg.split(" ");				
					drawMove(Integer.parseInt(tokens[4]),Integer.parseInt(tokens[5]),Integer.parseInt(tokens[1]));
					System.out.println(msg);
					msg = in.nextLine();
					
					while(msg.matches("Position [0-2] [0-2] is taken, try again")) {
						
						makeMove();
						msg = in.nextLine();	
					}
					if(msg.matches("Player [1-2] won!!!") || msg.matches("The game is a draw.")) {
						System.out.println(msg);
						break;
					}
					
									
					tokens = msg.split(" ");
					drawMove(Integer.parseInt(tokens[4]),Integer.parseInt(tokens[5]),Integer.parseInt(tokens[1]));
					System.out.println(msg);
					moveAgain = true;
					
				}
				if(msg.matches("Position [0-2] [0-2] is taken, try again")) {
					moveAgain = true;
				}
				if(msg.matches("Player [1-2] won!!!") || msg.matches("The game is a draw.")) {
					System.out.println(msg);
					moveAgain = false;//break
					//game over
				}

				if(msg.matches("Illegal player number") || msg.matches("Illegal Board Position")) {
					System.out.println(msg);
					moveAgain = true;
				}
				
				
			}while(moveAgain == true);
			
			
		} catch (UnknownHostException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		

	}
	private void makeMove() {
		
		Random rand = new Random();
		
		int row = rand.nextInt(3);
		int col = rand.nextInt(3);

		
		while(!legalMove(row, col)) {
			row = rand.nextInt(3);
			col = rand.nextInt(3);
		}
		
		out.println("move " + player + " " + row + " " + col);
		out.flush();
		
		//board[row][col] = 1;
	}
	private void drawMove(int row, int col, int player) {

		if(player == 1)
			gc.setFill(Paint.valueOf("blue"));
		if(player == 2)
			gc.setFill(Paint.valueOf("red"));
		gc.fillRect(0 + (BOX_SIZE * col), (BOX_SIZE * row), BOX_SIZE, BOX_SIZE);
		
		board[row][col] = player;
	}
	private boolean legalMove(int row, int col) {
		if(board[row][col] == -1)
			return true;
		else
			return false;
	}

}