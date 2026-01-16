import java.io.*;
import java.util.*;


public class Solution {
	
	public static int t,n,m;
	public static List<List<Integer>> shortnode, tallnode;
	public static List<Set<Integer>> s;
	public static void main(String[] args) throws IOException{
		BufferedReader br =new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st = new StringTokenizer(br.readLine());
		
		t= Integer.parseInt(st.nextToken());
		for(int tcnt=1;tcnt<=t;tcnt++) {
			int ans=0;
			st = new StringTokenizer(br.readLine());
			n=Integer.parseInt(st.nextToken());
			st = new StringTokenizer(br.readLine());
			m=Integer.parseInt(st.nextToken());
			tallnode=new ArrayList<>();
			shortnode = new ArrayList<>();
			s=new ArrayList<>();//한 점에서 출발 했을 때 지나가는 점들을 담을 set
			for(int i=0;i<=n;i++) {
				tallnode.add(new ArrayList<Integer>());
				shortnode.add(new ArrayList<Integer>());
				s.add(new HashSet<Integer>());
			}
			for(int i=0;i<m;i++) {
				st = new StringTokenizer(br.readLine());
				int a=Integer.parseInt(st.nextToken());
				int b=Integer.parseInt(st.nextToken());
				shortnode.get(a).add(b);
				tallnode.get(b).add(a);
			}
			for(int i=1;i<=n;i++) {
				boolean vis[]=new boolean[n+1];
				
				Queue<Integer> q = new ArrayDeque<>();
				q.add(i);
				while(!q.isEmpty()){
					int cur =q.poll();
					for(int dir=0; dir<shortnode.get(cur).size();dir++) {
						if(vis[shortnode.get(cur).get(dir)])	continue;
						vis[shortnode.get(cur).get(dir)]=true;
						s.get(i).add(shortnode.get(cur).get(dir));
						q.add(shortnode.get(cur).get(dir));
					}
				}
				vis=new boolean[n+1];
				
				Queue<Integer> qq = new ArrayDeque<>();
				qq.add(i);
				while(!qq.isEmpty()){
					int cur =qq.poll();
					for(int dir=0; dir<tallnode.get(cur).size();dir++) {
						if(vis[tallnode.get(cur).get(dir)])	continue;
						vis[tallnode.get(cur).get(dir)]=true;
						s.get(i).add(tallnode.get(cur).get(dir));
						qq.add(tallnode.get(cur).get(dir));
					}
				}
			}
			
			for(int i=1;i<=n;i++) {
				if(s.get(i).size()==n-1) {
					ans++;
				}
				
			}
			System.out.println("#"+tcnt+" "+ans);
		}
		
	}

}