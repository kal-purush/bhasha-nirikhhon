package kr.co.sist.market.vo;

public class ReqVO {
	
	private String id, itemCode;
	private int price;
	
	public ReqVO() {
	
	}
	
	public ReqVO(String id, String itemCode, int price) {
		this.id = id;
		this.itemCode = itemCode;
		this.price = price;
	}
	public String getItemCode() {
		return itemCode;
	}
	
	public void setItemCode(String itemCode) {
		this.itemCode = itemCode;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public int getPrice() {
		return price;
	}
	public void setPrice(int price) {
		this.price = price;
	}
	
	

}