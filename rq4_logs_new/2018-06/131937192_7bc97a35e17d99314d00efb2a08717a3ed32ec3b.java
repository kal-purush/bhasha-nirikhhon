package org.vashonsd.Games.SR;

public class Board {
    public Space[][] board;
    public Path[] paths;
    int count;
    //A board has 8 "paths" which are the combinations of square that represent a win.
    //Other than that the board is a 2d array of space objects.
    public Board() {
        paths = new Path[8];
        board=new Space[3][3];
        count=0;
        for(int i=0;i<3;i++){
            for(int c=0;c<3;c++){
                count++;
                board[i][c]= new Space(State.EMPTY,count);
            }
        }
        setGroupings();
    }



    public void setSpace(int x, int y, State state){
        board[x][y].setValue(state);
}
    //This is part of a future hope for this code where I clean up user input from the x and y cordinates
    public void setSpace(int num, State state){
        for(int i=0;i<3;i++){
            for(int c=0;c<3;c++){
              if(board[i][c].getNumber()==num){
                  board[i][c].setValue(state);
              }
            }
        }
    }
    public Space getSpace(int x, int y){
        return board[x][y];
    }

    //checks to see if the user has won by looping through all possible combinations of win
    public boolean xWins() {
        setGroupings();
        boolean isFalse=true;
        for(int i=0;i<8;i++) {
            if (paths[i].xWin()) {
                isFalse=false;
            }
        }
        if(isFalse){
            return false;
        }
        else{
            return true;
        }
    }
    //checks to see if the user has tied the computer by returning true if neither player has won and the board is full
    public boolean tie(){
        setGroupings();
        int y=0;
        for(int i=0;i<3;i++){
            for(int x=0;x<3;x++){
                if(board[x][i].getValue().equals(State.EMPTY)){
                    y++;
                }
            }
        }
        if(y>0||yWins()||xWins()){
            return false;
        }
        else{
            return true;
        }
    }
    //Checks to see if the computer has won by checking all possible combinations of win
    public boolean yWins() {
        setGroupings();
        boolean isFalse=true;
        for(int i=0;i<8;i++) {
            if (paths[i].oWin()) {
                isFalse=false;
            }
        }
        if(isFalse){
            return false;
        }
        else{
            return true;
        }
    }

    //turns one row from the board into a printable row
    public String toeFormat(Space[] spc){
        String a=spc[0].toString();
        String b=spc[1].toString();
        String c=spc[2].toString();
        return a +"|"+b+"|"+c;
    }

    //uses the toeFormat method to print a graphic representation of the game board
    public String toString(){
        Space[] one= new Space[3];
        Space[] two=new Space[3];
        Space[] three=new Space[3];
        for(int i=0;i<3;i++){
            one[i]=board[0][i];
            two[i]=board[1][i];
            three[i]=board[2][i];
        }

        return String.format(toeFormat(one)+"%n"+toeFormat(two)+"%n"+toeFormat(three));
    }

    //sets all of the paths to their coresponding place on the board, used to refresh the paths once players make moves
    public void setGroupings(){
        Path hOne= new Path(board[0][0],board[0][1],board[0][2]);
        paths[0]=hOne;
        Path hTwo = new Path(board[1][0],board[1][1],board[1][2]);
        paths[1]=hTwo;
        Path hThree = new Path(board[2][0],board[2][1],board[2][2]);
        paths[2]=hThree;
        Path vOne=new Path(board[0][0],board[1][0],board[2][0]);
        paths[3]=vOne;
        Path vTwo= new Path(board[0][1],board[1][1],board[2][1]);
        paths[4]=vTwo;
        Path vThree = new Path(board[0][2],board[1][2],board[2][2]);
        paths[5]=vThree;
        Path dOne=new Path(board[0][0],board[1][1],board[2][2]);
        paths[6]=dOne;
        Path dTwo= new Path(board[2][0],board[1][1],board[0][2]);
        paths[7] = dTwo;
    }



}