package monitor;

import java.lang.Thread;

public class BathroomThread extends Thread {
	
	Bathroom bathroom;
	int type;
	
	BathroomThread(Bathroom bathroom, int type) {
		this.bathroom = bathroom;
		this.type = type;
	}
	
	@Override
	public void run() {
		if(type == 1) {
			// call nex human
			while(true) {
				this.bathroom.callNext();
			}
		}
		else if(type == 2) {
			// change gender
			while(true) {
				try {
					Thread.sleep(4000);
				}
				catch(Exception e) {
					e.getMessage();
				}
				this.bathroom.changeGendeTimer();
			}
		}
		else {
			
		}
	}
}