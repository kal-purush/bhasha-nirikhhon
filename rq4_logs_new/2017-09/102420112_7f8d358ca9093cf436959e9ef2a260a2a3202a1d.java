
//*************************************************************************************
// COMMON DOUBLE FRAME

import java.awt.*;
import javax.swing.*;
import java.sql.*;
import java.awt.event.*;
import java.util.*;

public class CDFrame extends JFrame implements ActionListener
  {
  Container c;
  static int ix, iy;
  Object s,s2,s4,s6,s8;
  public static String s1,s3,s5,s7,s9;
  private Connection con;
  private ResultSet rs;
  private Statement stmt,stmt2;
  private ResultSetMetaData rsmd;
  public JTable table;
  JPanel jPanel1 = new JPanel();
  JPanel jPanel2 = new JPanel();
  JLabel jLabel1 = new JLabel();
  JButton jButton3 = new JButton();
  JButton jButton4 = new JButton();
  JButton jButton5 = new JButton();
  JLabel jLabel3 = new JLabel("CustomerID");
  JLabel jLabel4 = new JLabel(" CustomerProjectID");
  JLabel jLabel5 = new JLabel("                  ");
  JLabel jLabel6 = new JLabel("                  ");
  JLabel jLabel7= new JLabel("CustomerDescription  CustomerProjectDescription Order");
  JTextField jTextField3=new JTextField();
  JTextField jTextField4=new JTextField();

  JTextField jTextField8=new JTextField();
  JTextField jTextField9=new JTextField();
  JTextField jTextField10=new JTextField();
  JLabel jLabel8  = new JLabel("CustomerDescription");
  JLabel jLabel9  = new JLabel("CustomerProjectDesc");
  JLabel jLabel10 = new JLabel("OrderDate");
  JLabel jLabel20 = new JLabel(" Note:               Select the");
  JLabel jLabel21 = new JLabel("Customer ID Coloumn ");
  JLabel jLabel22 = new JLabel( "Alone");
  JLabel jLabel23 = new JLabel();
  JLabel jLabel24 = new JLabel();
// CONSTRUCTOR WITH ARGUMENT  
  public CDFrame(int x)
    {
    super("Customer Details");
    this.setLocation(150,150);
    this.setResizable(false);

    jLabel1.setText(" CustomerID CustomerProjectID");
    jLabel1.setBounds(new Rectangle(0, 0, 90, 22));
    jButton5.setText("Ok");
    jButton5.setBounds(new Rectangle(338, 24, 73, 24));
    jButton4.setText("Cancel");
    jButton4.setBounds(new Rectangle(195, 24, 73, 24));
    jButton3.setText("Save");
    jButton3.setBounds(new Rectangle(58, 24, 71, 23));
    jButton3.setToolTipText(" New Customer Details can be added using these Fields");

    jPanel1.setLayout(new FlowLayout(0,30,5));
    jPanel1.add(jLabel1);
    jPanel1.add(jLabel7);

    jPanel2.setLayout(new GridLayout(3,3));
    jPanel2.add(jLabel3);
    jPanel2.add(jLabel4);
    jPanel2.add(jLabel5);
    jPanel2.add(jTextField3);
    jPanel2.add(jTextField4);
    jPanel2.add(jLabel6);
    jPanel2.add(jButton3);
    jPanel2.add(jButton4);
    jPanel2.add(jButton5);
    	try
	      {
	       // Load the Driver class file
		 Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
	      // Make a connection to the ODBC datasource xoffice
		 con = DriverManager.getConnection( "jdbc:odbc:tms");//, "scott", "tiger");
	     }
        catch (SQLException sqle)
	      {
		System.err.println(sqle.getMessage());
	      }
	catch (ClassNotFoundException cnfe)
      	      {
		System.err.println(cnfe.getMessage());
	      }
    table =new JTable(4,4);
    c=getContentPane();
    c.setLayout(new BorderLayout());
    c.add( jPanel1,BorderLayout.NORTH );
    c.add( table,BorderLayout.CENTER);
    c.add( jPanel2,BorderLayout.SOUTH);
    setSize(500,400);
    jButton3.addActionListener(this);
    jButton4.addActionListener(this);
    jButton5.addActionListener(this);
    table.addMouseListener(new m());
    getTable();
    }
//CONSTRUCTOR
  public CDFrame()
    {
    super("Customer Details");
    this.setLocation(150,150);
    this.setResizable(false);
    jLabel1.setToolTipText("Select the Customer ID Coloumn Alone");
    jLabel1.setText(" CustomerID           "
                    +"  CustomerProjectID            "
                    +" CustomerDescription         "+
                    " CustProjDescription      Order");
    jLabel1.setBounds(new Rectangle(0, 0, 90, 22));
     jButton5.setText("Ok");
    jButton5.setBounds(new Rectangle(338, 24, 73, 24));
    jButton4.setText("Cancel");
    jButton4.setBounds(new Rectangle(195, 24, 73, 24));
    jButton3.setText("Save");
    jButton3.setBounds(new Rectangle(58, 24, 71, 23));
    jButton3.setToolTipText(" New Customer Details can be added using these Fields");

    jPanel1.setLayout(new FlowLayout(0,30,5));
    jPanel1.add(jLabel1);
    jPanel1.add(jLabel7);

    jPanel2.setLayout(new GridLayout(4,5));
    jPanel2.add(jLabel3);
    jPanel2.add(jLabel4);
    jPanel2.add(jLabel8);
    jPanel2.add(jLabel9);
    jPanel2.add(jLabel10);

    jPanel2.add(jTextField3);
    jPanel2.add(jTextField4);
    jPanel2.add(jTextField8);
    jPanel2.add(jTextField9);
    jPanel2.add(jTextField10);

    jPanel2.add(jLabel6);
    jPanel2.add(jButton3);
    jPanel2.add(jButton4);
    jPanel2.add(jButton5);
    jPanel2.add(jLabel5);

    jPanel2.add(jLabel20);
    jPanel2.add(jLabel21);
    jPanel2.add(jLabel22);
    jPanel2.add(jLabel23);
    jPanel2.add(jLabel24);
	    try
	      {
	       // Load the Driver class file
		Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
              // Make a connection to the ODBC datasource xoffice
		con = DriverManager.getConnection( "jdbc:odbc:tms");//, "scott", "tiger");
	     }
	   catch (SQLException sqle)
	      {
		System.err.println(sqle.getMessage());
	      }
	   catch (ClassNotFoundException cnfe)
	      {
		System.err.println(cnfe.getMessage());
	      }
    table =new JTable(4,4);
    c=getContentPane();
    c.setLayout(new BorderLayout());
    c.add( jPanel1,BorderLayout.NORTH );
    c.add( table,BorderLayout.CENTER);
    c.add( jPanel2,BorderLayout.SOUTH);
    setSize(700,400);
    getTable();
    show();
    jButton3.addActionListener(this);
    jButton4.addActionListener(this);
    jButton5.addActionListener(this);
    table.addMouseListener(new m());
    }
    class m extends MouseAdapter
      {
      public void mouseClicked(MouseEvent me)
        {
        iy=table.getSelectedColumn();
        ix=table.getSelectedRow();
        }
      }
// SAVE BUTTON
  public void actionPerformed(ActionEvent ae)
    {
      if(ae.getSource()== jButton3)
        {
          try
            {
            if( jTextField3.getText().equals("") || jTextField4.getText().equals(""))
              JOptionPane.showMessageDialog(null,"INFORMATION INADEQUATE");
            else
              {
              stmt2=con.createStatement();
              stmt2.executeUpdate("insert into customerdetails ( cid , cpid, "+
                                "ciddesc, cpiddesc, ord ) values "+
                                " ( '"+jTextField3.getText()+"' "+
                                ",'"+jTextField4.getText()+"'"+
                                ",'"+jTextField8.getText()+"'"+
                                ",'"+jTextField9.getText()+"'"+
                                " ,'"+jTextField10.getText()+"')" );
              JOptionPane.showMessageDialog(null,"SAVED SUCESSFULLY!");
              }
            }
          catch (SQLException sqle)
            {
	  	    System.err.println(sqle.getMessage());
	    }
        jTextField3.setText("");
        jTextField4.setText("");
        jTextField8.setText("");
        jTextField9.setText("");
        jTextField10.setText("");
        }
//CANCEL BUTTON
      if(ae.getSource()== jButton4)
        {
        jTextField3.setText("");
        jTextField4.setText("");
        jTextField8.setText("");
        jTextField9.setText("");
        jTextField10.setText("");
        }
      if(ae.getSource()== jButton5)
        {
        this.dispose();
        }
     }

  public String ret1()
      {
      s=table.getValueAt(ix,iy);
      s1=""+s;
      return s1;
      }

  public String ret2()
      {
      s2=table.getValueAt(ix,iy+1);
      s3=""+s2;
      return s3;
      }
  public String ret3()
      {
      s4=table.getValueAt(ix,iy+2);
      s5=""+s4;
      return s5;
      }
  public String ret4()
      {
      s6=table.getValueAt(ix,iy+3);
      s7=""+s6;
      return s7;
      }
   public String ret5()
      {
      s8=table.getValueAt(ix,iy+4);
      s9=""+s8;
      return s9;
      }
// TO GET THE TABLE FROM DATABASE
   private void getTable()
      {
      try
        {
        stmt=con.createStatement();
        rs=stmt.executeQuery( "select  * from customerdetails");
        displayResultSet(rs);
        }
      catch (SQLException sqle)
        {
  	  System.err.println(sqle.getMessage());
        }
      }
    private void displayResultSet(ResultSet rs1) throws SQLException
      {
        boolean morerec=rs1.next();
        if(!morerec)
          {
          JOptionPane.showMessageDialog(this,"Result set Contain No Records");
          return;
          }
        Vector ch=new Vector();
        Vector rows=new Vector();
        try
          {
            ResultSetMetaData rsmd1=rs1.getMetaData();
            for(int i=1;i<=rsmd1.getColumnCount();++i)
              ch.addElement(rsmd1.getColumnName(i));
            do
              {
              rows.addElement(getNextRow(rs1,rsmd1));
              }while(rs.next());
            table=new JTable(rows,ch);
            JScrollPane scr=new JScrollPane(table);
            Container c1=getContentPane();
            c1.remove(1);
            c1.add(scr,BorderLayout.CENTER);
            c1.validate();
          }
        catch (SQLException sqle)
          {
	   System.err.println(sqle.getMessage());
	  }
      }
  private Vector getNextRow(ResultSet rs2,ResultSetMetaData rsmd2) throws SQLException
    {
    Vector currentrow=new Vector();
    for(int i=1;i<= rsmd2.getColumnCount();++i)
      {
      switch(rsmd2.getColumnType(i))
        {
        case Types.VARCHAR :
        case Types.CHAR :
        case Types.LONGVARCHAR :
                                  currentrow.addElement(rs2.getString(i));
                                    break;
        case Types.DATE :
        case Types.TIME :
                                  currentrow.addElement(rs2.getDate(i));
                                   currentrow.addElement(rs2.getTime(i));
                                   break;
        case Types.INTEGER :
                                  currentrow.addElement(new Long(rs2.getInt(i)));
                                  break;
        default :
                  System.out.println("type was"+rsmd2.getColumnTypeName(i));
         }
      }
       return currentrow;
    }
   public void shutDown()
    {
    try
      {
      con.close();
      }
    catch (SQLException sqle)
      {
	System.err.println(sqle.getMessage());
      }
    }
  // MAIN FUNCTION
     public static void main(String args[])
      {
      final CDFrame b=new CDFrame();
       b.addWindowListener(new WindowAdapter()
                                  {
                                  public void windowClosing(WindowEvent e)
                                          {
                                          b.shutDown();
                                          System.exit(0);
                                          }
                                  }
                          );
      }
}