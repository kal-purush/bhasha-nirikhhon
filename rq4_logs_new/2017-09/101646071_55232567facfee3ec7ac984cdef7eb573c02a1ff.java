package builderPattern;

public class Main {

	public static void main(String[] args) {
		
		// 1. Main에서 객체 생성 시, Builder 생성 후 Nutrition 객체 .build() 한다. 
		Nutrition nutrition = new Nutrition
				.Builder(3,7)
				.calories(13)
				.carbohydrate(20)
				.fat(123)
				.sodium(3)
				.build();
		
		// 2. 값이 제대로 객체멤버 변수에 셋팅됬는지 확인한다. 
		System.out.println(nutrition.toString());
	}
}