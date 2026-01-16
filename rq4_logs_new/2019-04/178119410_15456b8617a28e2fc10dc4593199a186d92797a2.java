package Model;

import Viewer.*;


public class InputFile {
    private Integer lawnHeight;
    private Integer lawnWidth;
    private Location[] mowerLocations;
    private Direction[] mowerInitialDirections;
    private Location[] craterLocations;
    private Location[] puppyLocations;
    private String[] info;

    public void loadSetting(String[] file){info = file;}
    public int getLawnHeight(){return Integer.parseInt(info[0]);}
    public int getLawnWidth(){return Integer.parseInt(info[1]);}
    public Location[] getMowerLocations(){
        Location mower1 = new Location();
        mower1.setX(Integer.parseInt(info[2]));
        mower1.setY(Integer.parseInt(info[3]));
        Location mower2 = new Location();
        mower2.setX(Integer.parseInt(info[4]));
        mower2.setY(Integer.parseInt(info[5]));
        Location[] mowerLocations = new Location[2];
        mowerLocations[0] = mower1;
        mowerLocations[1] = mower2;
        return mowerLocations;
    }
    public Direction[] getMowerInitialDirections(){
        Direction mower1 = Direction.North;
        Direction mower2 = Direction.North;
        Direction[] directions = new Direction[2];
        directions[0] = mower1;
        directions[1] = mower2;
        return directions;
    }
    public Location[] getCraterLocations(){
        Location crater1 = new Location();
        crater1.setX(Integer.parseInt(info[6]));
        crater1.setY(Integer.parseInt(info[7]));
        Location crater2 = new Location();
        crater2.setX(Integer.parseInt(info[8]));
        crater2.setY(Integer.parseInt(info[9]));
        Location[] craterLocations = new Location[2];
        craterLocations[0] = crater1;
        craterLocations[1] = crater2;
        return craterLocations;
    }
    public Location[] getPuppyLocations(){
        Location puppy1 = new Location();
        puppy1.setX(Integer.parseInt(info[10]));
        puppy1.setY(Integer.parseInt(info[11]));
        Location puppy2 = new Location();
        puppy2.setX(Integer.parseInt(info[12]));
        puppy2.setY(Integer.parseInt(info[13]));
        Location[] puppyLocations = new Location[2];
        puppyLocations[0] = puppy1;
        puppyLocations[1] = puppy2;
        return puppyLocations;
    }
}
