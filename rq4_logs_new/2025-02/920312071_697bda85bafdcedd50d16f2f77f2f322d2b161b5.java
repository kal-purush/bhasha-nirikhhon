import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Queue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.StringTokenizer;
public class Main {
    
	public static int n,l,d;

    public static void main(String args[]) throws IOException{
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        n=Integer.parseInt(st.nextToken()); 
        l=Integer.parseInt(st.nextToken()); 
        d=Integer.parseInt(st.nextToken());
        List<Boolean> li = new ArrayList<>();
        for(int i=0 ;i<  n;i++) {
        	for(int j=0;j<l;j++) {
        		li.add(true);
        	}
        	if(i!=n-1) {
        		for(int j=0 ;j<5;j++) {
        			li.add(false);
        		}
        	}
        }
        
        int res=0;
        while(res<li.size()) {
        	if(li.get(res)==false) {
        		break;
        	}
        	res+=d;
        }
        System.out.println(res);

    }
}
