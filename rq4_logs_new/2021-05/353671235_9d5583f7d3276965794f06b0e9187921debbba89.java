
public class P2 {
	
	String breed;
	int age;
	String color;
	
	void hungry() {
		System.out.println("����Ŀ�");
	}
	void barking() {
		System.out.println("�۸�");
	}
	void sleeping() {
		System.out.println("zZ");
		
	}
	
	public static void main(String[] args) {
		P2 Dog;
		Dog = new P2();
		
		Dog.breed = "york";
		Dog.age = 1;
		Dog.color = "orange";
		Dog.hungry();
		Dog.barking();
		Dog.sleeping();
		
		System.out.println("(" + Dog.breed + "," + Dog.age + "," + Dog.color + ")");
	}
}
	