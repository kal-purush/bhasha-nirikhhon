package tr.com.dvm.model;

public class HistoricData {
	private PowerPlant pp;

	// Minute-based generation data (mWh)
	// As of the end of the minute, starting at the beginning of minute
	private String energy;

	// Date Time when the generation data was received (UNIX MilliSec.)
	private long receivedAt;
	
	public HistoricData(PowerPlant pp, String energy, long receivedAt) {
		this.pp = pp;
		this.energy = energy;
		this.receivedAt = receivedAt;
	}

	public PowerPlant getPp() {
		return pp;
	}

	public void setPp(PowerPlant pp) {
		this.pp = pp;
	}

	public String getEnergy() {
		return energy;
	}

	public void setEnergy(String energy) {
		this.energy = energy;
	}

	public long getReceivedAt() {
		return receivedAt;
	}

	public void setReceivedAt(long receivedAt) {
		this.receivedAt = receivedAt;
	}
}