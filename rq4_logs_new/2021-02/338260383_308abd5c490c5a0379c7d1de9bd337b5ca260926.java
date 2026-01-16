package TEST;

public class StringCalculator {

	static int Add(String s) {
		if (s.length() == 0)
			return 0;
      
		int a = s.indexOf(',');
		int num1 = 0, num2 = 0, res = 0;
		if (a != -1) {
			num1 = Integer.parseInt(s.substring(0, a));
			num2 = Integer.parseInt(s.substring(a + 1));
			res = num1 + num2;
		} else {
			res = Integer.parseInt(s);
		}
		return res;
	}
}