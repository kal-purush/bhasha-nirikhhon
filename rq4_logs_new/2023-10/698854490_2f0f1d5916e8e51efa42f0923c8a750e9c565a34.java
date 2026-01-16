
package edu.ulatina.objects;


public class UnitTO {
    
    private int plate;
    private String name;
    private int state;

    public UnitTO() {
    }

    public UnitTO(int plate, String name, int state) {
        this.plate = plate;
        this.name = name;
        this.state = state;
    }

    public int getPlate() {
        return plate;
    }

    public void setPlate(int plate) {
        this.plate = plate;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }
    
    
    
    
}