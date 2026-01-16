package uci.ics.mondego.tldr.model;

import java.util.ArrayList;
import java.util.List;

public class Method {
	
	

	private String name;
	private String fqn;
	private String returnType;
	private String signature;
	private List<String> holds;
	private List<String> parameter;
	private String body;
	
	public Method(String name,String fqn, String type, String signature, String value, String parameter){
		this.fqn = fqn;
		this.returnType = type;
		this.name = name;
		this.signature = signature;
		this.holds = new ArrayList<String>();
		this.parameter = new ArrayList<String>();
	}
	
	public Method(String name, String fqn, String type, String signature){
		this.name = name;
		this.fqn = fqn;
		this.returnType = type;
		this.signature = signature;
		this.holds = new ArrayList<String>();
		this.parameter = new ArrayList<String>();
	}
	
	public Method(){
		this.holds = new ArrayList<String>();
		this.parameter = new ArrayList<String>();
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
	
	public String getFqn() {
		return fqn;
	}
	public void setFqn(String fqn) {
		this.fqn = fqn;
	}
	public String getReturnType() {
		return returnType;
	}
	public void setType(String type) {
		this.returnType = type;
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
		sb.append("\nfqn" + this.fqn+"\n");
		sb.append("\nreturn type" + this.returnType+"\n");
		sb.append("\nholds" + this.holds+"\n");
		
		return sb.toString();
	}
	
	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public void setReturnType(String returnType) {
		this.returnType = returnType;
	}
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return super.hashCode();
	}
	
}