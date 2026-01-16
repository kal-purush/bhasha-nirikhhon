package ethicsandsecurity;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Random;

/**
 *   This class creates a random value lookup array of size 256. Each encryption
 *   routine will send the raw data points into the lookup array so that it can
 *   be used for encryption
 *   @author Joshua Barney
 */
public class Quantizer implements Serializable {
    public final double[] lookup;
    DecimalFormat df = new DecimalFormat("#.00");

    //initialize the quantizer class to create random lookup
    public Quantizer() {
        lookup = new double[256];
        for(int i = 0; i < 256; i++)
            setRandom(i);
    }

    //generate random values between -0.53 and 1.07
    public final void setRandom(int at){
        double min = -0.53;
        double max = 1.07;
        Random random = new Random();
        double range = max - min;
        double scaled = random.nextDouble() * range;
        double shifted = scaled + min;
        lookup[at] = Double.parseDouble(df.format(shifted));
    }

    //set a lookup position to an actual value
    public void setValue(int at, double in){
        lookup[at] = in;
    }

    //get the value from a specified position
    public double getValue(int at){
        return lookup[at];
    }

}