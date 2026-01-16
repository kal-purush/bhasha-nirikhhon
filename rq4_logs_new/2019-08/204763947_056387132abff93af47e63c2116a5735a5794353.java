
public class Pattern {
	public void m2(){
		System.out.print ("method2");
	}

	public static void main(String[] args) {
		
		int printNum=1;
		for(int i=1;i<=4;i++) {
			int addNum=0;
			if(i%2!=0 && i!=1) {
				printNum+=3;
				addNum=0;
			} else {
				addNum+=1;
			}
			
			for(int j=1;j<=i;j++)
			{
				System.out.print (printNum+addNum+" ");
				 
				
				
			}
			
			System.out.println("");
		}

	}

}