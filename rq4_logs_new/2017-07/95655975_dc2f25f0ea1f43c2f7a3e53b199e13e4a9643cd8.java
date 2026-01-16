package com.abhinav_tiwari;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class Check_Input_Id {
	
	static int emp_id;
	
	 BufferedReader br = new BufferedReader(new InputStreamReader((System.in)));
	public static int check_Duplicate_Id(int id, ArrayList<Employee_office_Details> emp_list) throws NumberFormatException, IOException
	{
		Check_Input_Id obj=new Check_Input_Id();
		//emp_id=id;
		obj.check(id,emp_list);		
		return emp_id;
		  
	}
	
	public  void check(int id, ArrayList<Employee_office_Details> emp_list) throws IOException
	{		
		emp_id=id;
		
		for(int j=0;j<emp_list.size();j++)
		{
			if(emp_list.get(j).getId()==emp_id)
			{
					System.out.println("Please Re-enter the id");
					try
					{
						emp_id=Integer.parseInt(br.readLine());
						check(emp_id,emp_list);	
					}
					catch(NumberFormatException e)
					{
						System.out.println(e);
						System.out.println("Please Re-enter the id");
						emp_id=Integer.parseInt(br.readLine());
						check(emp_id,emp_list);	
						//System.exit(0);
					}
					
			}
			
		}
		
	}
	 
	
}