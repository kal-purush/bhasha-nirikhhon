package be.upgrade.it.adventofcode2020.day12;

public class RainRisk {
    private final String[] navigation;
    private int x,y, xMultiplier, yMultiplier, heading = 0;

    public RainRisk(String[] navigation) {
        this.navigation = navigation;
        setHeading(1);
    }

    private void setHeading(int heading){
        this.heading = heading;
        switch (heading){
            case 0: //NORTH
                xMultiplier = 0;
                yMultiplier = 1;
                break;
            case 1: //EAST
                xMultiplier = 1;
                yMultiplier = 0;
                break;
            case 2: //SOUTH
                xMultiplier = 0;
                yMultiplier = -1;
                break;
            case 3: //WEST
                xMultiplier = -1;
                yMultiplier = 0;
                break;
        }
    }

    public int getManhattanDistance(){
        for (String s : navigation) {
            process(s);
        }


        return Math.abs(x) + Math.abs(y);
    }

    private void process(String s) {
        String command = s.substring(0, 1);
        int value = Integer.parseInt(s.substring(1));

        switch (command){
            case "E":
                x += value;
                break;
            case "N":
                y += value;
                break;
            case "S":
                y -= value;
                break;
            case "W":
                x -= value;
                break;
            case "F":
                x += (value * xMultiplier);
                y += (value * yMultiplier);
                break;
            case "L":
                int i = value / 90 % 4;
                int heading = this.heading - i;
                if(heading < 0){
                    heading+=4;
                }
                setHeading(heading);
                break;
            case "R":
                int i2 = value / 90 % 4;
                int heading2 = this.heading + i2;
                if(heading2 > 3){
                    heading2-=4;
                }
                setHeading(heading2);
                break;

        }
    }


}