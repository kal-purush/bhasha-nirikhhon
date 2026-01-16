import java.io.BufferedReader;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.io.IOException;

public class Main {
	public static int n,k;
	public static int arr[];
	public static int aa[];
	public static boolean vis[] = new boolean[10];
	public static StringBuilder sb =new StringBuilder();
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st = new StringTokenizer(br.readLine());
		n = Integer.parseInt(st.nextToken());
		k = Integer.parseInt(st.nextToken());
		arr = new int[n];
		aa = new int[n];
		st = new StringTokenizer(br.readLine());
		for(int i=0 ;i<n;i++) {
			aa[i]=Integer.parseInt(st.nextToken());

		}

		Arrays.sort(aa);

		dfs(0,0);
		
		System.out.println(sb);
	}

	public static void dfs(int d,int nxt) {
		if(d==k) {
			for(int i=0 ;i<k;i++) {
				sb.append(arr[i]).append(' ');
			}
			sb.append('\n');
			return;
		}
		
		for(int i=0; i<n ;i++) {
			if(vis[i])	continue;
			vis[i]=true;
			arr[d]=aa[i];
			dfs(d+1,i+1);
			vis[i]=false;
		}
	}

}