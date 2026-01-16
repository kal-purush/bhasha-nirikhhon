
public class Q01 {
	public static void main(String[] args){
		int result = 11;
		String decimal = "";
		String octal = "";
		String bynary = "";
		while(true){
			decimal = String.valueOf(result);
			octal = String.valueOf(Integer.toOctalString(result));
			bynary = String.valueOf(Integer.toBinaryString(result));
			if(reverse_check(decimal) && reverse_check(octal) && reverse_check(bynary)){
				System.out.println(result);
				break;
			}
			result += 2;
		}
	}
	
	public static boolean reverse_check(String nam){
			StringBuilder sb = new StringBuilder(nam);
			String reverse = sb.reverse().toString();
			if( nam.equals(reverse)){
				return true;
			}else{
				return false;
			}
	}
}