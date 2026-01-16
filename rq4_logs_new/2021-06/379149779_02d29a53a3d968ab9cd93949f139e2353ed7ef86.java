
import java.util.Objects;

/**
 *
 * This is the bulb class for holding data related to bulb it has following attributes
 * -> manufacturer
 * -> partNumber
 * -> wattage
 * -> lumens
 */
public class Bulb{
    
    //Attributes of bulb class
    private String manufacturer;
    private String partNumber;
    private int wattage;
    private int lumens;
    
    //A parameterized constructor that assigns the data using the parameters
    public Bulb(String manufacturer, String partNumber, int wattage, int lumens) {
        this.manufacturer = manufacturer;
        this.partNumber = partNumber;
        this.wattage = wattage;
        this.lumens = lumens;
    }
    
    //A getter which is used for getting manufacturer
    public String getManufacturer() {
        return manufacturer;
    }
    
    //A setter which is used for setting manufacturer
    public void setManufacturer(String manufacturer) {
        this.manufacturer = manufacturer;
    }

    //A getter for getting partNumber
    public String getPartNumber() {
        return partNumber;
    }

    //A setter for setting partNumber
    public void setPartNumber(String partNumber) {
        this.partNumber = partNumber;
    }

    //A getter for getting Wattage
    public int getWattage() {
        return wattage;
    }

    //A setter for setting Wattage
    public void setWattage(int wattage) {
        this.wattage = wattage;
    }

    //A getter for getting lumens
    public int getLumens() {
        return lumens;
    }
    
    //A setter for srtting lumens
    public void setLumens(int lumens) {
        this.lumens = lumens;
    }
    
    

    /**
     *
     * @param obj
     * @return boolean
     * This method is used for checking if two bulbs are equal
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Bulb other = (Bulb) obj;
        if (this.wattage != other.wattage) {
            return false;
        }
        if (this.lumens != other.lumens) {
            return false;
        }
        if (!Objects.equals(this.manufacturer, other.manufacturer)) {
            return false;
        }
        if (!Objects.equals(this.partNumber, other.partNumber)) {
            return false;
        }
        return true;
    }

    //This method is used for converting a bulb data to string that we can print using println
    @Override
    public String toString() {
        String retStr="";
        retStr += this.manufacturer+"\n";
        retStr += this.partNumber+"\n";
        retStr += this.lumens+"\n";
        retStr += this.wattage+"\n";
        retStr +="-*-*-*-*-*-*-*-*-*-*-*-\n";
        return retStr;
    }
}