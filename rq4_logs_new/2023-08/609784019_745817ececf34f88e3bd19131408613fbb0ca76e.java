package application;

import java.time.ZonedDateTime;

/*
 * Just a simple class for the activities with getters and setters 
 */

public class CalendarActivity {
    private ZonedDateTime date;
    private String clientName;
    private String doctorName;
    private Integer patiantId;

    public CalendarActivity(ZonedDateTime date, String clientName, Integer patientNo) {
        this.date = date;
        this.clientName = clientName;
        this.patiantId = patientNo;
    }

    public ZonedDateTime getDate() {
        return date;
    }

    public void setDate(ZonedDateTime date) {
        this.date = date;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public Integer getServiceNo() {
        return patiantId;
    }

    public void setServiceNo(Integer patientNo) {
        this.patiantId = patientNo;
    }

    @Override
    public String toString() {
        return "CalenderActivity{" +
                "date=" + date +
                ", clientName='" + clientName + '\'' +
                ", serviceNo=" + patiantId +
                '}';
    }

	public String getDoctorName() {
		return doctorName;
	}

	public void setDoctorName(String doctorName) {
		this.doctorName = doctorName;
	}
}