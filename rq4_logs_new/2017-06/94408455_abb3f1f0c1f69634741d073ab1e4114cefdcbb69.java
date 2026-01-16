package com.ac.expense;

import java.util.List;

public class ExpenseDetail {

	private String uid;
	// yyyyMMddHHmmss
	private String expenseTime;
	private boolean claimed;
	private List<MedicineDetail> medicines;

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getExpenseTime() {
		return expenseTime;
	}

	public void setExpenseTime(String expenseTime) {
		this.expenseTime = expenseTime;
	}

	public boolean isClaimed() {
		return claimed;
	}

	public void setClaimed(boolean claimed) {
		this.claimed = claimed;
	}

	public List<MedicineDetail> getMedicines() {
		return medicines;
	}

	public void setMedicines(List<MedicineDetail> medicines) {
		this.medicines = medicines;
	}

}