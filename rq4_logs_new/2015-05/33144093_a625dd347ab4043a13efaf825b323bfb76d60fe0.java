package trevorwarner.individualgame;

import android.widget.ImageButton;

/**
 * Created by BrickMaster logankruse11 on 4/29/2015.
 */
public class Brick {

    private int fullBrickHealth;
    private int currentBrickHealth;
    private int brickHits=0;
    private ImageButton brickButton;


    public Brick(int roundCount, ImageButton brickButton){
        this.fullBrickHealth=roundCount*roundCount+2;
        this.currentBrickHealth=fullBrickHealth;
        this.brickButton=brickButton;
        brickButton.setImageResource(R.drawable.brick);
    }

    public void setBrickHealth(int clickPower){
        currentBrickHealth-=clickPower;
        if(clickPower>currentBrickHealth) {
            brickHits += currentBrickHealth;
        }else{
            brickHits+=clickPower;
        }
        //If power less than remaining health?
        brickHits++;
        setBrickButton();
    }

    public void setBrickButton(){
        if(currentBrickHealth<=0){
            brickButton.setImageResource(R.drawable.pile_rocks);
        }else if(fullBrickHealth/3>=currentBrickHealth){
            brickButton.setImageResource(R.drawable.broken_brick);
        }else if((fullBrickHealth*2)/3>=currentBrickHealth){
            brickButton.setImageResource(R.drawable.cracked_brick);
        }
    }

    public int getCurrentBrickHealth(){return currentBrickHealth;}

    public ImageButton getBrickButton(){return brickButton;}

    public int getBrickHits(){return brickHits;}

}