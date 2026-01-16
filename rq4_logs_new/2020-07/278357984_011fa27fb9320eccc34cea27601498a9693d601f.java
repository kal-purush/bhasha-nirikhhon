package DAO;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Scanner;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;

public class UserDetails {

	public void viewUserDetails(int f)throws Exception
	{
		File myFile = new File("./Check.xlsx"); 
		FileInputStream fis = new FileInputStream(myFile);
		XSSFWorkbook myWorkBook = new XSSFWorkbook (fis);
		XSSFSheet ms = myWorkBook.getSheetAt(0);
		 System.out.println("Your Name: "+(ms.getRow(f).getCell(0)).toString());
         System.out.println("Your Id: "+(ms.getRow(f).getCell(1)).toString());
         System.out.println("Your Age: "+(ms.getRow(f).getCell(2)).toString());
         System.out.println("Your Position: "+(ms.getRow(f).getCell(3)).toString());
         System.out.println("Your Email:"+(ms.getRow(f).getCell(8)).toString());
         System.out.println("Your Address: "+(ms.getRow(f).getCell(4)).toString());

	}
	public void viewSalaryDetails(int f)throws Exception
	{
		File myFile = new File("./Check.xlsx"); 
		FileInputStream fis = new FileInputStream(myFile);
		XSSFWorkbook myWorkBook = new XSSFWorkbook (fis);
		XSSFSheet ms = myWorkBook.getSheetAt(0);
		double gross=0;
		double bs=Double.parseDouble((ms.getRow(f).getCell(5)).toString());
		//gross=basicSalary+medicalAllowance-(0.0367*basicSalary);
        System.out.println("Your Basic Salary: "+bs);
        System.out.println("Your Deduction: "+(ms.getRow(f).getCell(6)).toString());
        System.out.println("Your medicalAllowance:"+(4000));
        gross=bs+4000-(0.0367*bs);
        System.out.println("Your Gross Salary:"+(gross));
		 if(((gross*12)>=250000)&&((gross*12)<500000))
		        System.out.println("Your Tax%: 5%"+"\nYour Tax Amount:"+ (gross-(0.05*gross)));
		else if(((gross*12)>=500000)&&((gross*12)<1000000))
	        System.out.println("Your Tax%: 20%"+"\nYour Tax Amount:"+ (gross-(0.2*gross)));
		else if(((gross*12)>=1000000))
	        System.out.println("Your Tax%: 30%"+"\nYour Tax Amount:"+ (gross-(0.3*gross)));
		else 
	        System.out.println("Your Tax%: 0%"+"\nYour Tax Amount:"+ (0));
        System.out.println("Your Take Home Salary: "+(ms.getRow(f).getCell(7)).toString());
	}
	public void changeDetailsOfUser(int f)throws Exception
	{
		File myFile = new File("./Check.xlsx"); 
		FileInputStream fis = new FileInputStream(myFile);
		XSSFWorkbook myWorkBook = new XSSFWorkbook (fis);
		XSSFSheet ms = myWorkBook.getSheetAt(0);
		int kill=0;
		Scanner in=new Scanner(System.in);
		 while(kill!=1)
		   {
		   System.out.println("1.change Age");
		   System.out.println("2.change Address");
		   System.out.println("3.Change Email");
		   System.out.println("4.Exit");
		   System.out.println("Enter your choice:");
		   int ch=in.nextInt();
		   switch(ch)
		   {
		   case 1:
		   {
			   int agethere=Integer.parseInt(ms.getRow(f).getCell(2).toString());
			   System.out.println("Your Previous age: "+agethere);
			   System.out.println("Want to change?(y/n)");
			   char c=in.next().charAt(0);
			   if((c=='y')||(c=='Y'))
			   {
				   System.out.println("Enter Age:");
				   int agenew=in.nextInt();
			   XSSFRow row=ms.createRow(f);
			   Cell cell = row.createCell(2);
	               cell.setCellValue((Integer) agenew);
			   }
			   break;
		   }
		   case 2:
		   {
			   String addthere=(ms.getRow(f).getCell(4).toString());
			   System.out.println("Your Previous Address: "+addthere);
			   System.out.println("Want to change?(y/n)");
			   char c=in.next().charAt(0);
			   if((c=='y')||(c=='Y'))
			   {
				   System.out.println("Enter Address:");
				   String addnew=in.nextLine();
			   XSSFRow row=ms.createRow(f);
			   Cell cell = row.createCell(4);
	               cell.setCellValue((String) addnew);
			   }
			   break;
		   }
		   case 3:
		   {
			   String emailthere=(ms.getRow(f).getCell(8).toString());
			   System.out.println("Your Previous Email Id: "+emailthere);
			   System.out.println("Want to change?(y/n)");
			   char c=in.next().charAt(0);
			   if((c=='y')||(c=='Y'))
			   {
				   System.out.println("Enter Email Id:");
				   String emailnew=in.nextLine();
			   XSSFRow row=ms.createRow(f);
			   Cell cell = row.createCell(8);
	               cell.setCellValue((String) emailnew);
			   }
			   break;
		   }
		   case 4:
		   {
			   kill=1;
			   break;
		   }
		   default:
		   {
			   System.out.println("Wrong choice!! OOPS!!");
		   }
		   }
		   }
		   System.out.println("Your Name: "+(ms.getRow(f).getCell(0)).toString());
	       System.out.println("Your Id: "+(ms.getRow(f).getCell(1)).toString());
	       System.out.println("Your Age: "+(ms.getRow(f).getCell(2)).toString());
	       System.out.println("Your Position: "+(ms.getRow(f).getCell(3)).toString());
	       System.out.println("Your Email:"+(ms.getRow(f).getCell(8)).toString());
	       System.out.println("Your Address: "+(ms.getRow(f).getCell(4)).toString());
	}
	public void changePassword(int f)throws Exception
	{
		File myFile = new File("./Check.xlsx"); 
		FileInputStream fis = new FileInputStream(myFile);
		XSSFWorkbook myWorkBook = new XSSFWorkbook (fis);
		XSSFSheet ms = myWorkBook.getSheetAt(0);
		Scanner in=new Scanner(System.in);
		RegistrationValidation rv=new RegistrationValidation();
		   String passthere=(ms.getRow(f).getCell(9).toString());
		   int kill=0;int flag=0;
		   while(kill!=3)
		   {
			   System.out.println("Enter Prevoius Password");
			   String oldpass=in.nextLine();
			   if(oldpass.equals(passthere))
			   {
		   System.out.println("Want to change?(y/n)");
		   char c=in.next().charAt(0);
		   if((c=='y')||(c=='Y'))
		   {
			   while(flag!=1)
			   {
			   System.out.println("Enter New Password:");
			   String passnew=in.nextLine();
			   System.out.println("Enter Confirm Password:");
			   String passnew1=in.nextLine();
			   if(rv.validPassword(passnew, passnew1))
			   {
		   XSSFRow row=ms.createRow(f);
		   Cell cell = row.createCell(9);
            cell.setCellValue((String) passnew);
            flag=1;
			   }
			   else 
				   System.out.println("Enter a Strong Password....Check Whether both new and confirm password are same");
		   }
		   }
		   else
			   kill=3;
			   }
		   else
			   System.out.println("Incorrect Password");
            kill++;
		   
		   }
	}
	public void paySlipMaking(int f)throws Exception
	{
		File myFile = new File("./Check.xlsx"); 
		FileInputStream fis = new FileInputStream(myFile);
		XSSFWorkbook myWorkBook = new XSSFWorkbook (fis);
		XSSFSheet ms = myWorkBook.getSheetAt(0);
		  String name=(ms.getRow(f).getCell(0)).toString();
	       int id=Integer.parseInt((ms.getRow(f).getCell(1)).toString());
	       int age=Integer.parseInt((ms.getRow(f).getCell(2)).toString());
	       String position=(ms.getRow(f).getCell(3)).toString();
	       String email=(ms.getRow(f).getCell(8)).toString();
	       String address=(ms.getRow(f).getCell(4)).toString();
			double gross=0;
			double tax;
			double bs=Double.parseDouble((ms.getRow(f).getCell(5)).toString());
	        double deduction=Double.parseDouble((ms.getRow(f).getCell(6)).toString());
	        int medical=(4000);
	        gross=bs+4000-(0.0367*bs);
			 if(((gross*12)>=250000)&&((gross*12)<500000))
			        tax=(gross-(0.05*gross));
			else if(((gross*12)>=500000)&&((gross*12)<1000000))
		        tax=(gross-(0.2*gross));
			else if(((gross*12)>=1000000))
		        tax=(gross-(0.3*gross));
			else 
		        tax=0;
	        double take=Double.parseDouble((ms.getRow(f).getCell(7)).toString());
		Document document=new Document();
		try {
			PdfWriter w=PdfWriter.getInstance(document, new FileOutputStream("PaySlip.pdf"));
			document.open();
			document.add(new Paragraph("Name:"+name+"\nId:"+id+"\nPosition:"+position+"\nEmail Id:"+email+"\nAddress"+address+
					"\n\n\n\n-----------------YOUR SALARY-----------------\n"+"Basic salary:\t"+bs+"\nDeduction:\t+"+deduction
					+"\nMedical Al.:\t+"+medical+"\nGrossPay:\t="+gross+"\nTax:  \t-"+tax+"\nTakeHome:\t="+take));
			document.close();
			w.close();
		}
		catch(DocumentException e)
		{
			e.printStackTrace();
		}
		catch(FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}
}