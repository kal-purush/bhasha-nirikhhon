
import java.util.Scanner;

/**
 * Grant Hendricks
 *
 */


public class GameOfLife {

    public static void main( String[] args )    {
        System.out.println( "Welcome to Conway's Game of Life." );
        Scanner scan = new Scanner(System.in);
        System.out.println( "Please enter the size of the board?" );
        System.out.print( "rows: " );
        int rowSize = scan.nextInt();
        System.out.print( "columns: " );
        int columnSize = scan.nextInt();

        String [][] board = GenNewBoard(rowSize,columnSize);

        System.out.println( "How many generations do you want to see?" );
        System.out.print( "generations: " );
        int generations = scan.nextInt();

        //String [][] board = {{"_","#","#","_","_"}, {"_","#","_","#","_"}, {"_","#","#","_","#"} };

        for(int i=0; i<=generations; i++){
            System.out.println("Generation " + i + ":");
            //printing board
            for (int rows =0; rows<rowSize; rows++){
                for (int columns=0; columns<columnSize; columns++){
                    System.out.print(board[rows][columns] + "");
                }
                System.out.println("");
            }
            board = life(board, rowSize, columnSize);
        }
        System.out.println( "       " );
        System.out.println( "    GAME OVER    " );
        System.out.println( "       " );
        System.out.println( "    Thank You for Playing    " );
    }

    public static String[][] life(String [][] board, int rowSize, int columnSize){

        String [][] newBoard = new String[rowSize][columnSize];

        for (int row =0; row< rowSize; row++){

            for (int i=0; i<columnSize; i++ ){ /*i = columns*/

                if(board[row][i] =="#"){
                    //if space is populated
                    int neighbourCount = neighbourCounter(board, row, i, rowSize, columnSize);
                    if(neighbourCount<=1){
                        newBoard[row][i] = "_"; //is dead
                    }
                    if(neighbourCount==2 || neighbourCount==3 ){
                        newBoard[row][i] = "#"; //is alive
                    }
                    if(neighbourCount>=4 ){
                        newBoard[row][i] = "_"; //is dead
                    }
                }
                if(board[row][i] =="_"){
                    //if space is unpopulated
                    int neighbourCount = neighbourCounter(board, row, i, rowSize, columnSize);
                    if(neighbourCount==3 ){
                        newBoard[row][i] = "#"; //is alive
                    }
                    else {
                        newBoard[row][i] = "_"; //is dead
                    }

                }

            }
        }

        return newBoard;
    }

    public static String [][] GenNewBoard(int rowSize, int columnSize){
        //String [][] newBoard = new String [][] {{"_","#","#","_","_"}, {"_","#","_","#","_"}, {"_","#","_","#","_"}, {"_","#","_","#","_"}, {"_","#","#","_","#"} };

        String [][] newBoard = new String [rowSize][columnSize] ;

        for (int rows =0; rows<rowSize; rows++){
            for (int columns=0; columns<columnSize; columns++){
                if ((Math.random()*10)>4){
                    newBoard[rows][columns] = "#";
                }
                else {
                    newBoard[rows][columns] = "_";
                }
            }
        }
        return newBoard;
    }

    public static int neighbourCounter(String [][] board, int x, int y, int rowSize, int columnSize){
        int neighbourCount = 0;
        if(y!=0){
            if(x!=0){
                if(board[x-1][y-1] == "#"){neighbourCount++;}           //Start at bottom left
            }
            if(board[x][y-1] == "#"){neighbourCount++;}
            if(x!=rowSize-1){
                if(board[x+1][y-1] == "#"){neighbourCount++;}
            }
        }
        if(x!=0){
            if(board[x-1][y] == "#"){neighbourCount++;}
        }
        //if(board[x][y] == "#"){neighbourCount++;}     must not count the original
        if(x!=rowSize-1){
            if(board[x+1][y] == "#"){neighbourCount++;}
        }
        if(y!=columnSize-1){
            if(x!=0){
                if(board[x-1][y+1] == "#"){neighbourCount++;}
            }
            if(board[x][y+1] == "#"){neighbourCount++;}
            if(x!=rowSize-1){
                if(board[x+1][y+1] == "#"){neighbourCount++;}           //End at top right
            }
        }

        return neighbourCount;
    }
}
