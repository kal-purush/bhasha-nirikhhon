package Models;

import javafx.beans.property.SimpleStringProperty;

public class DrugsSearch {
	private SimpleStringProperty DrugName;
	private SimpleStringProperty Brand;
	private SimpleStringProperty Qty;
	private SimpleStringProperty Supplier;
	private SimpleStringProperty Date;
	public DrugsSearch(String DrugName,String Brand,String Qty,String Supplier,String Date) {
		this.DrugName= new SimpleStringProperty(DrugName);
		this.Brand= new SimpleStringProperty(Brand);
		this.Qty= new SimpleStringProperty(Qty);
		this.Supplier= new SimpleStringProperty(Supplier);
		this.Date= new SimpleStringProperty(Date);
	}
	public SimpleStringProperty getDrugName() {
		return DrugName;
	}
	public void setDrugName(String drugName) {
		this.DrugName.set(drugName);
	}
	public SimpleStringProperty getBrand() {
		return Brand;
	}
	public void setBrand(String brand) {
		this.Brand.set(brand);
	}
	public SimpleStringProperty getQty() {
		return Qty;
	}
	public void setQty(String qty) {
		this.Qty.set(qty);
	}
	public SimpleStringProperty getSupplier() {
		return Supplier;
	}
	public void setSupplier(String supplier) {
		this.Supplier.set(supplier);
	}
	public SimpleStringProperty getDate() {
		return Date;
	}
	public void setDate(String date) {
		this.Date.set(date);
	}
}