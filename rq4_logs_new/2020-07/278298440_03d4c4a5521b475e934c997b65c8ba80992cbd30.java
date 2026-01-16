package Email;

import java.util.Scanner;

public class Email {
	private String firstname;
	private String lastname;
	private String password;
	private String department;
	private int mailboxcapacity=500;
	private String changepassword;
	private String alternateemail;
	private int defaultpasswordlength=8;
	private String email;
	//constructor to ask firstname and lastname
	public Email(String firstname,String lastname)
	{
		this.firstname=firstname;
		this.lastname=lastname;
		//System.out.println("EMAIL CREATED:"+this.firstname+this.lastname);
		//create a method asking for department
		this.department=setDepartment();
		//System.out.println("Deaprtment:"+this.department);
		//call a method to create default password
		this.password=setRandompassword(defaultpasswordlength);
		//System.out.println("Your Password is:"+this.password);
		email=firstname.toLowerCase()+"."+lastname+"@"+"."+department+".com";
		//System.out.println("Your emailid is: "+email);
		
	}
	//ask for department
	private String setDepartment()
	{
		System.out.println("DEPARTMENT CODES: \n 1.Sales \n 2.Development \n 3.Accounting\n 4.None\n");
		System.out.println("Please enter department code");
		Scanner in = new Scanner(System.in);
		int deptChoice = in.nextInt();
		if(deptChoice==1)
			return "sales";
		else if(deptChoice==2)
			return "Development";
		else if(deptChoice==3)
			return "Accounting";
		else
			return "";
		
	}
	//generate random password
	private String setRandompassword(int length)
	{
		String passwordSet="ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890@#_-*%./\"";
		char password[]=new char[length];
		for(int i=0;i<length;i++)
		{
			int rand=(int)(Math.random()*(passwordSet.length()));
			password[i]=passwordSet.charAt(rand);
		}
		return new String(password);
			
	}
	//set mailbox capacity
	public void setmailboxcapacity(int capacity)
	{
		this.mailboxcapacity=capacity;
	}
	//set alternate email
	public void setalternateemail(String alternate)
	{
		this.alternateemail=alternate;
	}
	//set new password
	public void setnewpassword(String pass)
	{
		this.changepassword=pass;
	}
	public int getmailboxcapacity()
	{
		return mailboxcapacity;
	}
	public String getalternateemail()
	{
		return alternateemail;
	}
	public String getnewpassword()
	{
		return changepassword;
	}
	public String showinfo()
	{
		return "Name: "+firstname+" "+lastname+" \n"+"Department: "+department+" \n"+"Password: "+password+" \n"+"Email: "+email;
	}

}