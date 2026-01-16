package omar;
import java.util.Random; 
public class Test1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		for(int ii=0; ii<2; ii++) {
				System.out.print("\n");
		
		for(int i=1; i<=4; i++) {
		
		
		String dtr="qwertyuioplkjhgfdsazxcvbnm";
		
		Random r=new Random();
		int g=r.nextInt(dtr.length());
		
		System.out.print(dtr.charAt(g));
		
		}
		}
		
		
		
		
		
		
	}

}