package bowling;

import java.io.IOException;

public class NumberEntryException extends IOException {
    public NumberEntryException(int line){
	super("Number players and number of entry in line "+line+" do not match");
    }
}