import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;



public class ArrayListExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ArrayList<String> al = new ArrayList<String>();
		al.add("madhu");
		al.add("vimal");
		al.add("sangeetha");
		ArrayList<String> al1 = new ArrayList<String>();
		al1.add("swati");
		al1.add("raj");
		al1.add("vimal");
		al1.add("madhu");
//		al.retainAll(al1);
		al1.remove(1);
		System.out.println(al1);
		al.addAll(0,al1);
		Collections.sort(al);
//		Iterator itr=al.iterator();  
//		  while(itr.hasNext()){  
//		   System.out.println(itr.next());  
//		  }  
		  System.out.println(al);
	}

}