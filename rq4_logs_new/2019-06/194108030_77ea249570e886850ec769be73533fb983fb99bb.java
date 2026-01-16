package JavaSessions;

public class DuplicateElements {

	public static void main(String[] args) {
		
		
		String [] names = {"selenium", "java", "python", "java"};
		
		for(int i=0; i<names.length;i++) {
			
			for(int j=i+1; j<names.length;j++) {
				
				if(names[i].equals(names[j])) {
					
					System.out.println("Duplicate element is::"+names[i]);
				}
			}
		}
		
		
	}

}