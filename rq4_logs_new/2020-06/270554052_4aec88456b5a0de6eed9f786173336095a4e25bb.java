package chap11;
import java.util.*; //자료구조 로딩 임폴트

public class Car implements Cloneable{
	private String modelName;
	private ArrayList<String> owners = new ArrayList<String>();
	public String getModelName() {	//modelName 반환
		return this.modelName;
	}
	public void setModelName(String modelName) {	//modelName 설정
		this.modelName = modelName;
	}
	public ArrayList getOwners() {	//owners 반환
		return this.owners;
	}
	public void setOwners(String ownerName) {	//owners 값 추가
		this.owners.add(ownerName);
	}
	public Object clone() {	//반환형태가 Object임
		try {
			//Object클래스의 clone()매소드를 이용하여 복제된 Car클래스의 colonedCar 인스턴스
			Car clonedCar = (Car) super.clone();	
			return clonedCar;
			
		} catch(CloneNotSupportedException ex){
			ex.printStackTrace();
			return null;
		}
		
	}
	@Override
	protected void finalize() throws Throwable {
		System.out.println("Car의 객체에 대한 finalize가 실행됨");
	}
	
}