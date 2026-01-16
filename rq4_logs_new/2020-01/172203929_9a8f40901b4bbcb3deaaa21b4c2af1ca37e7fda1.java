package file.read.write;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Driver {

	public static void main(String[] args) throws IOException {
		String source="C:\\Users\\RUPAM\\Desktop\\write.txt";
		int lenghts=45;
		ReadTextFile read= new ReadTextFile();
		WriteTextFile write=new WriteTextFile();
		
		String allData=read.readFile(source);
		List<String> dataList=new ArrayList<String>(Arrays.asList(allData.split(" ")));
		write.Customwrite(".//temp.txt", allData);
		String result="";
		for(int i=0;i<lenghts;i++)
		{
			
			try {
				result+=dataList.get(0)+" ";
				dataList.remove(0);
			}
			catch(java.lang.IndexOutOfBoundsException a) {
				// TODO: handle exception
				System.out.println("handle");
			}
			
		}
		System.out.println(result);
		write.Customwrite(source, String.join(" ", dataList));
		
		//replace source file to its original state
		String data=read.readFile(".//temp.txt");
		write.Customwrite(source, data);;
	}

}