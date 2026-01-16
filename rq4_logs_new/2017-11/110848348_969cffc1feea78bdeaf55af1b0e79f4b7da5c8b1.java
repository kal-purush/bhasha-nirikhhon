/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package HHSSadventure;

/**
 *
 * @author bonsk5852
 */
public class LOL {

    private String direction;
    private String imgName;
    private String isBlocked;
    private String isNext;
    private String nextDirection;

    // constructor
    public LOL(String direction, String imgName, String isBlocked, String isNext, String nextDirection) {
        this.direction = direction;
        this.imgName = imgName;
        this.isBlocked = isBlocked;
        this.isNext = isNext;
        this.nextDirection = nextDirection;
    }

    public String getDirection() {
        return this.direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public String getImgName() {
        return this.imgName;
    }

    public void setImgName(String n) {
        this.imgName = n;
    }

    public boolean isBlocked(String blocked) {
        if (blocked.equalsIgnoreCase("false")) {
            return false;
        } else {
            return true;
        }
    }

    public void setBlocked(String b) {
        this.isBlocked = b;
    }

    public String isNext() {
        return this.isNext;
    }

    public void isNext(String n) {
        this.isNext = n;
    }

    public String getNextDirection() {
        return this.nextDirection;
    }

    public void setNextDirection(String direction) {
        this.nextDirection = direction;
    }
}