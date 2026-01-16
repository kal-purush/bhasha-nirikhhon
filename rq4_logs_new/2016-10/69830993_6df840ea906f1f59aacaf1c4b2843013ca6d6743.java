package myDentist.model;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.Table;


@Entity
@Table(name = "MakeAvailability")
public class MakeAvailability {

	@Id
	@GeneratedValue
	private Integer id; 
	
    public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	
	private String availableDate;
	
	@OneToOne
	private Doctor doctorId;
	
	
	private boolean slot910;
	private boolean slot1011;
	private boolean slot1112;
	private boolean slot121;
	private boolean slot12;
	private boolean slot23;
	private boolean slot34;
	private boolean slot45;
	
	public String getAvailableDate() {
		return availableDate;
	}
	public void setAvailableDate(String availableDate) {
		this.availableDate = availableDate;
	}
	public Doctor getDoctorId() {
		return doctorId;
	}
	public void setDoctorId(Doctor doctorId) {
		this.doctorId = doctorId;
	}
	public boolean isSlot910() {
		return slot910;
	}
	public void setSlot910(boolean slot910) {
		this.slot910 = slot910;
	}
	public boolean isSlot1011() {
		return slot1011;
	}
	public void setSlot1011(boolean slot1011) {
		this.slot1011 = slot1011;
	}
	public boolean isSlot1112() {
		return slot1112;
	}
	public void setSlot1112(boolean slot1112) {
		this.slot1112 = slot1112;
	}
	public boolean isSlot121() {
		return slot121;
	}
	public void setSlot121(boolean slot121) {
		this.slot121 = slot121;
	}
	public boolean isSlot12() {
		return slot12;
	}
	public void setSlot12(boolean slot12) {
		this.slot12 = slot12;
	}
	public boolean isSlot23() {
		return slot23;
	}
	public void setSlot23(boolean slot23) {
		this.slot23 = slot23;
	}
	public boolean isSlot34() {
		return slot34;
	}
	public void setSlot34(boolean slot34) {
		this.slot34 = slot34;
	}
	public boolean isSlot45() {
		return slot45;
	}
	public void setSlot45(boolean slot45) {
		this.slot45 = slot45;
	}

}