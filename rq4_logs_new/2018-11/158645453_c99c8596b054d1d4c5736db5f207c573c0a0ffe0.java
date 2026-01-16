package com.vvb.myPack;
import java.util.*;

public interface Converter{
double doConvert();
}

class ConverterManager implements Converter{
	int flag;
	Scanner sc;
	public double doConvert(){
		sc=new Scanner(System.in);
		System.out.println("Select option \n 1.InchToFeet \n 2.FeetToInch");
		flag=sc.nextInt();
		if(flag==1){
			System.out.println("Enter Inch value:\n");
			double inch=sc.nextDouble();
			return inch*0.0833;
		}
		else{
			System.out.println("Enter feet value:\t");
			double feet=sc.nextDouble();
			return feet*12;
		}
		
		}

	public static Converter getInchFeetConverter() {
		return new ConverterManager();
	}
}