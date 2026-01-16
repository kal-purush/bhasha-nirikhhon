package uci.ics.mondego.tldr.model;

import java.util.ArrayList;
import java.util.List;

public class LocalVariable {

	private String name;
	private String type;
	private String signature;
	private List<String> holds;
	
	public LocalVariable(String name, String type, String signature){
		this.type = type;
		this.name = name;
		this.signature = signature;
		this.holds = new ArrayList<String>();
	}
	
	
	public LocalVariable(){
		this.holds = new ArrayList<String>();
	}
	
	
	public void addHold(String h){
		holds.add(h);
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	
	public String getType() {
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
	}
	
	public String getSignature() {
		return signature;
	}
	
	public void setSignature(String signature) {
		this.signature = signature;
	}
	
	
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("\nname" + this.name+"\n");
		sb.append("\ntype" + this.type+"\n");
		sb.append("\nholds" + this.holds+"\n");
		
		return sb.toString();
	}
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return super.hashCode();
	}	

}