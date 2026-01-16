package jul25;

public class Item {
	//인스턴스 변수
	//상품 코드(문자열)
	//상품 이름(문자열)
	//상품 가격(정수)
	public String code;
	public String name;
	public int price;
	//등록할 때 사용하는 메서드(:생성자 Constructor)를 선언한다
	//= 클래스를 인스턴스(instance)로 바꾸어주는 메서드
	//생성자는 void 메서드도 아니고 리턴 메서드도 아닌 별개의 것
	//생성자를 만드는 방법 : 생성자의 이름은 클래스 이름과 같다
	public Item(String code, String name, int price){
		this.code = code;
		this.name = name;
		this.price = price;
	}//<-생성자!
}