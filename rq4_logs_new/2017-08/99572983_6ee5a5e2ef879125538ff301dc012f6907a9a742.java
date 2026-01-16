package matsimrouting;

public class FrequencyData {

	String hId;	
	String tripId;
	String frequency;
	public FrequencyData(String hId, String tripId, String frequency) {
		this.hId = hId;
		this.tripId = tripId;
		this.frequency = frequency;
	}
	
	@Override
	public String toString() {
		return hId + ";" + tripId +";" + frequency;
	}
}