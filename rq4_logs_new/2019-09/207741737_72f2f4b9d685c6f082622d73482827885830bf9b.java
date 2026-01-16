//测试地址：http://www.dooccn.com/java/

import java.io.*;
import java.util.ArrayList;
class test  
{
   //思想：先根据数字的映射字符串，进行两两组合，得出新的组合数组后，再依次和后面输入的字符串进行组合，最终得出结果
   public static void main (String[] args) throws java.lang.Exception
	{
		 round1();
		 System.out.println(" ");
		 round2();
	}

	 public static void round1 ()
	 {
		String[] strArray={"","","abc","def","ghi","jkl","mno","pqrs","tuv","wxyz"}; //映射字符串
		int arr[]={2,3};//给定的整数数组

		System.out.println("===============round1 begin======================");
		String[] ret = {};//初始结果为空
		int i;
		for(i=0; i<arr.length; i++)
		{
			//结果数组再和后面输入的字符串进行组合，得出新的结果数组备用
			ret = compose(ret,  strArray[arr[i]]);
		}

		//打印结果
		for(i=0; i<ret.length; i++)
		{
			System.out.print(ret[i]);
			
			if(i % 10 == 9) System.out.println(" ");  //每10个结果打印一次换行
			else System.out.print(" ");
		}
		System.out.println(" ");
		System.out.println("===============round1 end======================");
	 }

	 public static void round2 ()
	 {
		String[] strArray={"","","abc","def","ghi","jkl","mno","pqrs","tuv","wxyz"}; //映射字符串
		int arr[]={0,1,2,3,4,9};//给定的整数数组

		System.out.println("===============round2 begin======================");
		String[] ret = {};//初始结果为空
		int i,k;
		for(i=0; i<arr.length; i++)
		{
			//结果数组再和后面输入的字符串进行组合，得出新的结果数组备用
			k=arr[i];
			if(k >= strArray.length){
				System.out.println("错误：数字"+k+"超出映射范围!");
				return;
			}
			ret = compose(ret,  strArray[arr[i]]);
		}

		//打印结果
		for(i=0; i<ret.length; i++)
		{
			System.out.print(ret[i]);
			
			if(i % 10 == 9) System.out.println(" ");  //每10个结果打印一次换行
			else System.out.print(" ");
		}
		System.out.println(" ");
		System.out.println("===============round2 end======================");
	 }

	//把字符串数组和字符串进行组合，并返回组合后的字符串数组
	//输入参数arr为待组合的数组，
	//输入参数str为要组合的字符串
	//主要就是把输入str打散为数组，然后和输入参数arr两两组合，形成新的组合数组，然后返回
	  public static String[] compose(String[] arr, String str)
	  {
	      if(str.length() == 0) return arr;
		  String tmp ="";
		  
		  ArrayList<String> list = new ArrayList<String>();
		  if(arr == null || arr.length == 0)
		  {
			   for(int j=0; j<str.length(); j++)
				{
					tmp = String.valueOf(str.charAt(j));
					list.add(tmp);
				}
		  }
	     else
		 {
			  for(int i=0; i<arr.length; i++)
			  { 
				for(int j=0; j<str.length(); j++)
				{
					tmp = arr[i] + String.valueOf(str.charAt(j));
					list.add(tmp);
				}
			  }
		 }
	     return list.toArray(new String[list.size()]);
	  }

}