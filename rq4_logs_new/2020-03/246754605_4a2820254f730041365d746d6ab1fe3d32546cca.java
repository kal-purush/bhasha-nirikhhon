package chapter20;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class BufferInput {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("BufferedReader in ��ü ���� ");
		System.out.println("������ �Է����ּ��� : ");

		String s = "";
		try {
			s = in.readLine();

		} catch (Exception e) {
			System.out.println("Error : " + e.toString());
		}
		
		System.out.println(s);

	}

}