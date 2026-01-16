import java.util.*;

public class LargestNumber {
		private static boolean isGreaterorEqual(String d,String mD)
	{
		
			
		d=d.toString()+mD.toString();
		mD=mD.toString()+d.toString();
		if(d.compareTo(mD)>=0)
			return true;
		else
			return false;
	}
	private static String largestNumber(String[] a) {
		//write your code here

		List<String> result = new ArrayList<>();
		List<String> list = new ArrayList<String>(Arrays.asList(a));
		
		
		while(!list.isEmpty())
		{	
			String mD="";
			for(String d : list)
			{
				if(isGreaterorEqual(d,mD))
					mD=d;
				
			}
			result.add(mD);
			list.remove(mD);

		}
		
		String[] r=result.toArray(new String[result.size()]);
		String res="";
		for(int i=0;i<r.length;i++)
		{
			res=res+r[i];
		}

		return res;
	}
	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		int n = scanner.nextInt();
		String[] a = new String[n];
		for (int i = 0; i < n; i++) {
			a[i] = scanner.next();
		}
		System.out.println(largestNumber(a));
	}
}
