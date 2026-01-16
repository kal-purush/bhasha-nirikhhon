import java.io.*;
import java.util.*;

class Tugas1{
	public static void main(String []args){
		Scanner obj = new Scanner(System.in);
		String a = obj.next();
		String b = obj.next();

		try{
			BufferedWriter bw = new BufferedWriter(
				new FileWriter("D:\\File\\"+b+".txt"));
			BufferedReader br = new BufferedReader(
				new FileReader("D:\\File\\"+a+".txt"));
			String s;
			while((s = br.readLine()) != null){
				bw.write(s + "\n");
			}
			br.close();
			bw.close();
			System.out.print("Berhasil");
		}catch(Exception e){
			System.out.print("Gagal");
			return;
		}
	}
}