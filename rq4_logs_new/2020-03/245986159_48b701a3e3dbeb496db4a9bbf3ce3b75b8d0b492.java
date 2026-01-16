package lk.sack.livescore.beans;

public class Player {
    String name;
    boolean isBatting;

    public Player(){

    }

    public void setName(String name){
        this.name = name;
    }

    public String getName(){
        return this.name;
    }

    public void setIsBatting(boolean isBatting){
        this.isBatting = isBatting;
    }

    public boolean getIsBatting(){
        return this.isBatting;
    }
}
