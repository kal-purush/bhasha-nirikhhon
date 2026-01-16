public class InsuranceCalculationHelper{
  
	public static float conditionTest(float fr){

		if (fr < 0f){
			System.out.println(" 1 ok");
			return 0f;}
		
		float result = 0f;
		if (fr > 500f){
			System.out.println(" 2  ok");
			result = 500f;
		} else {
			System.out.println(" 3  ok");
			result = fr * 2.0f;}
		
		if (result <1.0f){
			System.out.println(" 4  ok");
			result = 1.0f;}
		
		System.out.println(" 5  ok");
		return result;
	}
}	