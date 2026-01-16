import java.applet.Applet;
import java.awt.Button;
import java.awt.Graphics;
import java.awt.Label;
import java.awt.TextField;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.*;


public class Form extends Applet implements ActionListener{
	TextField name,age,email_id;
	Label l1,l2,l3,l4;
	Button b1; 
	
	public void paint(Graphics g)
	{
		setLayout(null);
		g.drawString("name", 10, 10); 
		g.drawString("age", 10, 30);
		g.drawString("Email_ID", 10, 50);
			
	}
	public void init()
	{
		setLayout(null);
		name=new TextField();
		age=new TextField();
		email_id=new TextField();
		l1=new Label("*");
		l2=new Label("*");
		l3=new Label("*");
		l4=new Label("");
		b1=new Button("Okay");
		name.setBounds(60, 0, 50, 20);
		age.setBounds(60, 20, 50, 20);
		email_id.setBounds(60, 40, 50, 20);
		b1.setBounds(40, 70, 50, 20);
		l1.setBounds(120, 0, 100, 20);
		l2.setBounds(120, 20, 100, 20);
		l3.setBounds(120, 40, 100, 20);
		l4.setBounds(50, 130, 120, 20);
	    add(l1);
	    add(l2);
	    add(l3);
	    add(l4);
		add(b1);
		add(name);
		add(age);
		add(email_id);
		
		b1.addActionListener(this);
		
		
	}
	

	@Override
	public void actionPerformed(ActionEvent arg0) {
		// TODO Auto-generated method stub
		
		
		String check_name=name.getText();                //check name
		int b=check_name.length();
		 int flag1=0,flag2=0;
		 int d=check_name.codePointAt(0);
		   if(d<65 || d>90)
		     flag1=1;
		for(int i=1;i<b;i++)
		{ int e=check_name.codePointAt(i);
		  if(e<97 || e>122)
		     flag2=1;  
		}

		if (flag1==1 || flag2==1)
		l1.setText("Improper_name");
		else
		l1.setText("Proper_name");
		
		String check_age=age.getText();                     //check age
		for(int i=0;i<check_age.length();i++)
		{
			int k=check_age.codePointAt(i);
			if(k<48 || k>57)
			l2.setText("Wrong age");
			else
				l2.setText("Correct age");
		}
		
		String check_email=email_id.getText();
		int b1=check_email.indexOf("@");
		int f1=0,f2=0;
		if(b1==-1 || b1==0)
		{ f1=1; }
		int c=check_email.indexOf("@",b1+1);
		if (c!=-1)
		{  f1=1;  }
		if(f1==0)
		{
		int d1=check_email.indexOf(".",b1);
		if(d1==1 || d1==2)
		{l3.setText("Wrong format");  f2=1; }
		}
		if(f1==0 && f2==0)
		 l3.setText("Correct email");
		
		
		try {
		Class.forName("com.mysql.jdbc.Driver");
		Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/Rahul","","");
		Statement st=con.createStatement();
		if(l1.getText()=="Proper_name" && l2.getText()=="Correct age" && l3.getText()=="Correct email")
		{ l4.setText("Saved to database");
		st.executeUpdate("insert into log values('"+name.getText()+"','"+age.getText()+"','"+email_id.getText()+"')");
		}
		
	}
	catch(Exception e){System.out.println(e);
	}
		
	}

}