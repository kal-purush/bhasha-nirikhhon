import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class Main {

	public static List<Long> arr = new ArrayList<Long>();
	public static List<Long> tmp = new ArrayList<Long>();
	public static List<Long> ans = new ArrayList<Long>();
	public static int v,p;
	public static long l;
	public static long sum=Long.MAX_VALUE;
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st = new StringTokenizer(br.readLine());
		v = Integer.parseInt(st.nextToken());
		p = Integer.parseInt(st.nextToken());
		l = Long.parseLong(st.nextToken());
		st = new StringTokenizer(br.readLine());
		for(int i=0 ;i<v;i++) {
			arr.add(Long.parseLong(st.nextToken()));
		}
		dfs(0,0);
		System.out.println(sum);
		for(int i=0 ;i<p;i++) {
			System.out.print(ans.get(i)+" ");
		}
	}
	
	public static void dfs(int cnt,int nxt) {
		
		if(cnt==p) {
			long t =0;
			for(int i=0 ;i<v;i++) {
				long a=Long.MAX_VALUE;
				for(int j =0; j<cnt;j++) {
					long dis =Math.abs(tmp.get(j)-arr.get(i));
					a = Math.min(a,Math.min(dis,l - dis));
				}
				t+=a;
			}

			if(t<sum) {
				sum =t;
				ans.clear();
				ans.addAll(tmp);
				
			}
				
			return;
		}
		
		
		for(int i=nxt ;i<v;i++) {
			tmp.add(arr.get(i));
			dfs(cnt+1,i+1);
			tmp.remove(tmp.size()-1);
		}
	}
}