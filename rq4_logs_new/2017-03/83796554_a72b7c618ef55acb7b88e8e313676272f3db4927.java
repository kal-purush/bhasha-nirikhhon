
import java.util.*;
import java.io.*;

public class MajorityElement {
	private static int getfrequency(int a[],int m)
	{
		int count=0;
		for(int i=0;i<a.length;i++)
		{
			if(a[i]==m)
				count++;
			if (count > a.length / 2) break;
		}
		return count;
		
		
		
	}
	 private static int getMajorityElement(int[] a, int left, int right) {
	        if (left == right) {  // right is n, i.e. a.length
	            return -1;
	        }
	        if (left + 1 == right) {
	            return a[left];
	        }
	        return major(a, left, right - 1);
	    }
    private static int major(int[] a, int low, int high) {
       
        if (low == high) return a[low];
        int mid = (high - low) / 2 + low;
        int ans  = major(a, low, mid);
        int was = major(a, mid + 1, high);
        if (ans == was) return ans;
        
        int left_count  = getfrequency(a, ans);
        int right_count = getfrequency(a, was);
        
        return left_count > a.length / 2 ? ans :
            ( right_count > a.length / 2 ? was : -1 );
    }
   

	private static int Moore(int a[],int low,int high)
	{
		int count=0,majority=-1;
		for(int i=low;i<high;i++)
		{
			if (count == 0) {
                majority = a[i];
                count = 1;
            }
            else if (a[i] == majority) count++;
            else if (a[i] != majority) count--;
			
		}
		count = 0;
        // stills needs to check if really is majority
        for (int i = low; i < high; i++) {
            if (a[i] == majority) count++;
            if (count > high / 2) return majority;
        }
        return -1;
	}



	public static void main(String[] args) {
        FastScanner scanner = new FastScanner(System.in);
        int n = scanner.nextInt();
        int[] a = new int[n];
        for (int i = 0; i < n; i++) {
            a[i] = scanner.nextInt();
        }
        if (Moore(a, 0, a.length) != -1) {
            System.out.println(1);
        } else {
            System.out.println(0);
        }
    }
    static class FastScanner {
        BufferedReader br;
        StringTokenizer st;

        FastScanner(InputStream stream) {
            try {
                br = new BufferedReader(new InputStreamReader(stream));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        String next() {
            while (st == null || !st.hasMoreTokens()) {
                try {
                    st = new StringTokenizer(br.readLine());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return st.nextToken();
        }

        int nextInt() {
            return Integer.parseInt(next());
        }
    }
}
