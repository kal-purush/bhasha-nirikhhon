package model;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "medicine")
public class Medicine {

	@Id
	@GeneratedValue
	private Integer medicineId;
	private String medicineName; 
	private Long medicinePrice;
	
	
	public Integer getMedicineId() {
		return medicineId;
	}
	
	public void setMedicineId(Integer medicineId) {
		this.medicineId = medicineId;
	}
	
	public String getMedicineName() {
		return medicineName;
	}
	
	public void setMedicineName(String medicineName) {
		this.medicineName = medicineName;
	}
	
	public Long getMedicinePrice() {
		return medicinePrice;
	}
	
	public void setMedicinePrice(Long medicinePrice) {
		this.medicinePrice = medicinePrice;
	}
	
	
}