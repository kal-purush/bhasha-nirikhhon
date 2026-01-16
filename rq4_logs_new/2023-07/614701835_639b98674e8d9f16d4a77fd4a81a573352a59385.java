import java.awt.*;
import java.awt.event.*;
import java.applet.*;
import javax.swing.*;
import java.sql.*;
class DemoMain extends Frame implements ActionListener
{
	JLabel im;
	MenuBar mb;
	Menu DataEntry,Report,Help;
	static int ty_no=1;	

	MenuItem  customer,jobcard,monthreport,worker,aboutus,bill,purchase,polo, rawmat;

	DemoMain()
	{
	super("TYRE RETREADING FACTORY MANAGEMENT");
		setLayout(null);
		mb= new MenuBar();
		setMenuBar(mb);
		ImageIcon ii=new ImageIcon("ghh.jpg");
		im=new JLabel(ii);
		im.setBounds(0,15,1025,730);
		add(im);
		DataEntry=new Menu("DataEntry");
		customer=new MenuItem("Customer DataEntry");
		worker=new MenuItem("Worker DataEntry");
		jobcard=new MenuItem("JobCard DataEntry");
		rawmat=new MenuItem("Raw Material Report");
		purchase=new MenuItem("RawMaterial purchase DataEntry");		
		monthreport=new MenuItem("Monthwise RawMaterial used DataEntry");
	
addWindowListener(new WindowAdapter(){
public void windowClosing(WindowEvent e)
{
System.exit(0);
}
});
		Help=new Menu("Help");
		aboutus=new MenuItem("AboutUs");
		
		Report=new Menu("Report");
		bill=new MenuItem("Bill Report");
		polo=new MenuItem("Profit & Loss Report");

		DataEntry.add(customer);
		//DataEntry.add(jobcard);
		//DataEntry.add(monthreport);
		DataEntry.add(worker);
		DataEntry.add(purchase);

		Report.add(bill);
		
		Report.add(polo);
		Report.add(rawmat);
		Report.add(monthreport);
		Help.add(aboutus);
		

		mb.add(DataEntry);		
		mb.add(Report);	
		mb.add(Help);	

		customer.addActionListener(this);
		purchase.addActionListener(this);
		worker.addActionListener(this);
		//jobcard.addActionListener(this);
		monthreport.addActionListener(this);
		bill.addActionListener(this);
		aboutus.addActionListener(this);
	
		polo.addActionListener(this);
		rawmat.addActionListener(this);
	}

public void getvalue(int ty_no1)
{
ty_no=ty_no1;
System.out.println("Tyre numbers "+ty_no);
}

	public void actionPerformed(ActionEvent ae)
	{

		MenuItem m=(MenuItem)ae.getSource();

		if(m==customer)
		{
			JCustomer jc=new JCustomer("Customer Data Entry");
			jc.setSize(1025,735);	
			jc.setVisible(true);
		}
		else if(m==rawmat)
		{
		RawReport rr=new RawReport();
		rr.setSize(1000,800);
		rr.setVisible(true);
		}
		else if(m==monthreport)
		{
			
		Month rm=new Month();
		rm.setSize(800,800);	
		rm.setVisible(true);
		}
		/*else if(m==jobcard)
		{
//	int i;
//	do{
//	i=1;
		JobCard jt=new JobCard("Job Card Data Entry");
		jt.setSize(800,800);	
		jt.setVisible(true);
//	i++;
//	}while(i<=ty_no);
		}*/
		else if(m==worker)
		{
			JWorker jw=new JWorker("Worker Data Entry");
			jw.setSize(800,800);	
			jw.setVisible(true);
		
		}
		else if(m==bill)
		{
		
			JBill1 b=new JBill1("Billing Report");
			b.setVisible(true);
			b.setSize(700,800);
		}
		else if(m==purchase)
		{

		Purchase p=new Purchase("Raw Material Purchase Details");
		p.setSize(800,800);	
		p.setVisible(true);
		}
		else if(m==polo)
		{
			JProfitloss polo=new JProfitloss("Profit & Loss Report");
			polo.setSize(600,600);	
			polo.setVisible(true);
		}
		else if(m==aboutus)
		{
				
			JHelp h=new JHelp("About Us");
			h.setSize(400,400);	
			h.setVisible(true);
	
		}
	}
	       public static void main(String args[])
	{
		DemoMain dm=new DemoMain();
		dm.setSize(1025,735);	
		dm.setVisible(true);
	}
}
