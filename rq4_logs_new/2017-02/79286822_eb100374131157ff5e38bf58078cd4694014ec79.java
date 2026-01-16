package edu.oregonstate.cs361.battleship;


/**
 * Filename: newModel
 * Project: Battleship
 * Created by ksteinfeldt on 2/3/17.
 */

public class Ship {
    String type;
    int size;
    int x_coord;
    int y_coord;
    int orientation;

    /**************
    Class Functions
    ***************/
    // Getters
    public int getSize(){
        return size;
    }

    public int getX_coord(){
        return x_coord;
    }

    public int getY_coord(){
        return y_coord
    }

    public int getOrientation(){
        return orientation
    }

    // Setters
    public void setSize(int in){
        size = in;
    }

    public coords setCoords(int xin, int yin){
        x_coords = xin;
        y_coo
    }


    public void setOrientation(int in){
        orientation = in;
    }

    // Ship Constructor
    public Ship(string id){
        if(id == 'submarine'){
            type = "Submarine";
            size = 2
        }
        else if(id == 'destroyer'){
            type = "Destroyer";
            size = 2;
        }
        else if(id == 'cruiser'){
            type = "Cruiser";
            size = 3;
        }
        else if(id == 'battleship'){
            type = "Battleship";
            size = 4;
        }
        else if(id == 'aircraftCarrier') {
            type = "AircraftCarrier";
            size = 5;
        }


    }

}