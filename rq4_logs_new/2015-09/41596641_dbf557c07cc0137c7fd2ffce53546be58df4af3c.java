import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StreamTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class BalloonRise {
	public static void main(String[] args) throws IOException{
		StreamTokenizer in =new StreamTokenizer(new BufferedReader(new InputStreamReader(System.in)));
		PrintWriter out=new PrintWriter(new OutputStreamWriter(System.out));		
		while(true){
			in.nextToken();
			int N=(int)in.nval;
			if(N==0)break;
			Map<String,Integer> balloon=new HashMap<String,Integer>();
			for(int i=0;i<N;i++){
				in.nextToken();
				String color=in.sval;
				if(balloon.containsKey(color)){
					int count=balloon.get(color);
					balloon.put(color, count+1);
				}else{
					balloon.put(color, 1);
				}
			}
			String maxcolor = null;
			int maxcount=0;
			for(Entry<String,Integer> entry:balloon.entrySet()){
				if(entry.getValue()>maxcount){
					maxcolor=entry.getKey();
					maxcount=entry.getValue();
				}
			}
			out.println(maxcolor);
		}
		out.flush();
	}
}