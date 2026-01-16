package study;


/*
	object 의 toString(), hashCode(), equals() 메소드에 대해 조사하기 (오버라이딩하기)
 */
class Card {
	private int number;
	private String type;
	public Card(int number, String type) {
		this.number = number;
		this.type = type;
	}
	
	public int getNumber() {
		return this.number;
	}
	
	public String getString() {
		return this.type;
	}
	
	
	@Override
	public String toString() {
		return "이 카드의 타입은 " + this.type + " 입니다.";
	}
	
	
	// 오버라이딩 해서 재정의하지 않으면 아래에서 card1 card4 비교하면 false 값이 나온다.
	// 하지만 여기에서 오바리이딩해서 수정하면 객체 값을 비교해서 true 가 나오게 할 수 있다. 
	@Override
	public boolean equals(Object obj) {
		if(this.getNumber() ==((Card)obj).getNumber() && this.getString() == ((Card)obj).getString()) {
			return true;
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		int result = this.getString().hashCode() + this.getNumber();
		return result;
	}

	
	
}



public class HomeStudy1 {

	public static void main(String[] args) {
		Card card1 = new Card(1,"A");
		Card card2 = new Card(2,"B");
		Card card3 = new Card(3,"C");
		Card card4 = new Card(1,"A");
		
		// toString 재정의해주지 않으면 객체의 참조변수값?? 이상한 수가 나온다.
		// toString 오버라이딩 해주면 원하는 값을 출력할 수 있다.
		System.out.println(card1.toString());
		
		// String 비교는 값을 비교하기 때문에 아래 예제에서는 true가 나온다.
		// String 클래스의 equals 는 String 에서 Object의 equals 메소드를 재정의를 한것.
		String a = "abc";
		String b = "abc";
		System.out.println("a와 b 비교 : " + a.equals(b));
		
		System.out.println("================================");

		// Card 클래스에서 equals 를 재정의(오버라이드) 해서 객체가 가진 값을 비교하게 수정해주면
		// 값이 같을 때 true가 나오게 할 수 있다.
		// 원래는 false 출력. equals를 수정해주면 true로 나오게 할 수 있다.
		System.out.println("card1(1,'A') card4(1,'A') 비교 : " + card1.equals(card4) );
		
		System.out.println("================================");
		
		// String 클래스의 hashCode() 비교. 
		System.out.println("a hashCode() 값 : " + a.hashCode());
		System.out.println("b hashCode() 값 : " + b.hashCode());
		
		// 오버라이드한 hashCode() 비교.
		System.out.println("card1 hashCode() 값 : " + card1.hashCode());
		System.out.println("card4 hashCode() 값 : " + card4.hashCode());

	}

}