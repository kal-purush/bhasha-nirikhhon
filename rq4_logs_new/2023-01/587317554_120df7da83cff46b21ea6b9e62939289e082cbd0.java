package college;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
public class College2
{
@SuppressWarnings({ "unchecked", "rawtypes" })
public static void main(String[] args) throws IOException, 
SQLException {
boolean flag=true;
{
try (Scanner sc = new Scanner(System.in)) {
 while(flag){
System.out.println("1.INSERT INTO TABLE");
System.out.println("2.UPDATE IN TABLE");
System.out.println("3.READ FROM TABLE");
System.out.println("4.DELETE FROM TABLE");
System.out.println("5.EXIT");
int n=sc.nextInt();
switch(n)
{
case 1:
Connection 
con=DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb","root","
root");
PreparedStatement ps=con.prepareStatement("insert into 
College2 values(?,?,?,?)"); 
Scanner br=new Scanner(System.in); 
do{ 
System.out.println("Enter student name:"); 
String stu_name=br.next(); ; 
System.out.println("Enter SAP ID:"); 
int sap_id=br.nextInt(); 
System.out.println("Enter subject name:"); 
String subject_name=br.next();
System.out.println("Enter student's cgpa:"); 
double cgpa=br.nextDouble(); 
ps.setString(1,stu_name); 
ps.setInt(2,sap_id);
ps.setString(3,subject_name);
ps.setDouble(4,cgpa); 
int i = 0;
i = ps.executeUpdate(); 
System.out.println(i+" records affected");
System.out.println("Do you want to continue: y/n"); 
String s=br.next(); 
if(s.startsWith("n")){ 
break; 
}
}while(true);
break;
case 2:
Connection 
con2=DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb","root",
"root");
BufferedReader br2=new BufferedReader(new
InputStreamReader(System.in)); 
PreparedStatement ps1=con2.prepareStatement("UPDATE 
College2 set cgpa=? where stu_name=?");
System.out.println("Enter the updated cgpa:");
double cgpa=Double.parseDouble(br2.readLine()); 
System.out.println("Enter student name you want to 
update cgpa for:");
String stu_name=br2.readLine();
ps1.setDouble(1, cgpa);
ps1.setString(2, stu_name);
ps1.executeUpdate();
System.out.println("Values updated");
break;
case 3:
Connection 
con4=DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb","root",
"root");
PreparedStatement pstmt1 = 
con4.prepareStatement("Select * from College2");
ResultSet rs= pstmt1.executeQuery();
ResultSetMetaData md = rs.getMetaData();
 int columns = md.getColumnCount();
 ArrayList list = new ArrayList(50);
 while (rs.next()){
 HashMap row = new HashMap(columns);
 for(int i=1; i<=columns; ++i){ 
 row.put(md.getColumnName(i),
 rs.getObject(i));
 }
 list.add(row);
 System.out.println(row);
 }
 break;
case 4:
Connection 
con3=DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb","root",
"root");
BufferedReader br3=new BufferedReader(new
InputStreamReader(System.in)); 
PreparedStatement ps2=con3.prepareStatement("delete 
from College2 where stu_name=?");
System.out.println("Enter the student name you want 
to delete");
String stu_name1=br3.readLine(); 
ps2.setString(1,stu_name1);
ps2.executeUpdate();
System.out.println("Deletion complete");
break;
case 5:
System.out.println("Exited successfully.");
flag = false;
break;
}
} 
}
catch (NumberFormatException e)
{
e.printStackTrace();
}
}
}
}