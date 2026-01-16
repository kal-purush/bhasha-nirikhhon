
import java.util.*;
import java.io.*;
public class Main {

	static int n;
	static List<Character> li =new ArrayList<>();
	static List<Integer> num = new ArrayList<>();
	static boolean vis[]=new boolean[10];
	static StringBuilder sb =new StringBuilder();
	public static void main(String[] args) throws IOException{
		BufferedReader br =new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st =new StringTokenizer(br.readLine());
		
		n=Integer.parseInt(st.nextToken());
		st=new StringTokenizer(br.readLine());
		for(int i=0 ;i< n ;i++) {
			li.add(st.nextToken().charAt(0));
		}

		dfs(0);
		
		for(int i=sb.length()-n-2;i<sb.length();i++) {
			System.out.print(sb.charAt(i));
		}
		
		for(int i=0;i<n+1;i++) {
			System.out.print(sb.charAt(i));
		}


	}
	
	static void dfs(int cnt) {
		
		if(cnt==n+1) {
			int a =num.get(0);
			boolean flag=false;
			for(int i=0;i<li.size();i++) {
				if(li.get(i)=='<') {
					if(num.get(i+1)<=a) {
						flag=true;
						break;
					}
					
				}
				else if(li.get(i)=='>') {
					if(num.get(i+1)>=a) {
						flag=true;
						break;
					}
				}
				a=num.get(i+1);
			}
			
			if(!flag) {
				for(int i=0;i<num.size();i++) {
					sb.append(num.get(i));
				}
				sb.append('\n');
			}
			
			return;
		}
		
		for(int i=0;i<10;i++) {
			if(vis[i])	continue;
			vis[i]=true;
			num.add(i);
			dfs(cnt+1);
			vis[i]=false;
			num.remove(cnt);
			
		}
	}

}