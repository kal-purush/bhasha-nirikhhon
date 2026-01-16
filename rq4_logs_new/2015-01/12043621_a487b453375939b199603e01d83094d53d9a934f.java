import java.util.Vector;

public class phone {
	public static void main(String argv []){
		int n1=3,n2=7,n3=5;
		
		int i;
		Vector v1 = new Vector<Integer>();
		for(i=0; i<;10; i++){
			v1.add(i);			
		}
		System.out.println(v1);
		v1.removeElement(Integer.valueOf(n1));
		v1.removeElement(Integer.valueOf(n2));
		v1.removeElement(Integer.valueOf(n3));
		System.out.println(v1);
		for(i=0; i<v1.size(); i++){
			addPhoneNumber(String.valueOf(v1.get(i)), v1,3);			
		}
	}
	
	public static void addPhoneNumber(String src,Vector<Integer> allowed,int rem){
		int i,k;
		if(rem == 0){
			/*
			 *  check for 4
			 */
		}
		else{
			k=0;
			String org = src;
			int temp = rem;
			while(k<allowed.size()){
				src = org;
				rem = temp;
				for(i=k; i<k+rem; i++){
					if(!src.equals("4") && allowed.get(i%allowed.size())==4){
						src = "4" + src;
						rem--;
					}
					else if(Integer.parseInt(src) == allowed.get(i%allowed.size())){
						rem++;
						
					}
					else{
						src = src + String.valueOf(allowed.get(i%allowed.size()));
						rem--;
					}
					
				}
				System.out.println(src);
			k++;
			}
		}
		
	}
}