/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
//package library;

/**
 *
 * @author Group44
 */
import java.io.*;
import java.io.File;
import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.* ;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Scanner;



public class Library {
    
     //user category initialization
    static int ucid;
    static int max;
    static int period;
    
    //book category initialization
    static int bcid;
    static String bcname;
    
    //libuser initialization
    static String libuid;
    static String name;
    static int age;
    static String address;
   //static string ucid
    
    //borrow initialisation
    static String callnum;
    static int copynum;
    //static String libuid;
    static String checkout_temp;
    static String returndate_temp;
    //static Date checkout;
    //static Date returndate;
    
    //book initialisation
    static String title;
    //static Date publish;
    static String publish_temp;
    static String rating_temp;
    static float rating;
    static int tborrowed;
    
    //authorship
    static String aname;
    
    //--------------------------------------CONNECTION SETUP------------------------------------------------------------------------//   
    public static Connection connect(){
        String dbAddress = "jdbc:mysql://projgw.cse.cuhk.edu.hk:2633/db44";
        String dbUsername = "Group44";
        String dbPassword = "CSCI3170";
        
        try
            {
                Class.forName("com.mysql.jdbc.Driver");
                return DriverManager.getConnection(dbAddress, dbUsername, dbPassword);
            } 
        catch(ClassNotFoundException |SQLException e)
            {
                System.out.println("[Error]: Java MySQL DB Driver not found!!");
                return null;
            }   
    }
    
 //-------------------------------------CREATE TABLES-------------------------------------------------------------------------//
    
    public static void create_user_category(){
    
        String a= "create table user_category("
        + "ucid integer,"
        + "max integer not null,"
        + "period integer not null,"
        + "constraint primary key(ucid));";   
        
        try (Connection con = connect();
             Statement stmt=con.createStatement()) {
             stmt.executeUpdate(a); 
        }catch (SQLException ex) {
             System.out.println("[Error]: "+ex.getMessage());
        }
    }
    
    public static void create_libuser(){
        
        String b= "create table libuser("
           + "libuid char(10),"
           + "name char(25) not null,"
           + "age integer not null,"
           + "address char(100) not null,"
           + "ucid integer not null,"
           + "constraint primary key (libuid),"
           + "foreign key (ucid) references user_category(ucid));";

       try (Connection con = connect();
           Statement stmt=con.createStatement()) {
           stmt.executeUpdate(b); 
       }catch (SQLException ex) {
           System.out.println("[Error]: "+ex.getMessage());
       }
    
    }
    
    public static void create_book_category(){
          
        String c="create table book_category("
                    + "bcid integer,"
                    + "bcname char(30) not null,"
                    + "constraint primary key (bcid));";
        try (Connection con = connect();
             Statement stmt=con.createStatement()) {
             stmt.executeUpdate(c);
        }catch (SQLException ex) {
             System.out.println("[Error]: "+ex.getMessage());
        }
    
    }
    
    public static void create_book(){
        String d="create table book("
               + "callnum char(8),"
               + "title char(30) not null,"
               + "publish date null,"                              
               + "rating real null,"
               + "tborrowed integer not null,"
               + "bcid integer not null,"
               + "foreign key (bcid) references book_category (bcid),"
               + "constraint primary key (callnum));";
        try (Connection con = connect();
             Statement stmt=con.createStatement()) {
             stmt.executeUpdate(d);
        }catch (SQLException ex) {
             System.out.println("[Error]: "+ex.getMessage());
        }
    }
    
    public static void create_copy(){
        
        String e="create table copy("
                + "callnum char(8),"
                + "copynum integer,"
                + "constraint primary key (callnum, copynum),"
                + "foreign key (callnum) references book(callnum));"; 
        try (Connection con = connect();
             Statement stmt=con.createStatement()) {
             stmt.executeUpdate(e);
        }catch (SQLException ex) {
             System.out.println("[Error]: "+ ex.getMessage());
        }
    }
    
    public static void create_borrow(){
        
            String f="create table borrow("
                    + "callnum char(8),"
                    + "copynum integer,"
                    + "libuid char(10),"
                    + "checkout date,"
                    + "returndate date null,"
                    + "foreign key (callnum,copynum) references copy(callnum,copynum),"
                    + "foreign key (libuid) references libuser(libuid),"
                    + "constraint primary key (callnum,copynum,libuid,checkout));";
            try (Connection con = connect();
                Statement stmt=con.createStatement()) {
                stmt.executeUpdate(f);
            }catch (SQLException ex) {
             System.out.println("[Error]: "+ex.getMessage());
            }
            
    }
    
    public static void create_authorship(){
            
            String g= "create table authorship("
                      + "callnum char(8),"
                      + "aname char(25),"               
                      + "foreign key (callnum) references book(callnum),"
                      + "constraint primary key (callnum,aname));"; 
            try (Connection con = connect();
                Statement stmt=con.createStatement()) {
                stmt.executeUpdate(g);
                System.out.println("Processing. . .Done. Database is initialized.\n");
            }catch (SQLException ex) {
                System.out.println("[Error]: "+ex.getMessage());
            }
    }
    
    public static void create_table(){
        
        create_user_category();
        create_libuser();
        create_book_category();
        create_book();
        create_copy();
        create_borrow();
        create_authorship();
    
    }
    
    //--------------------------------------------DELETE TABLES----------------------------------------------------------------------//
    public static void delete_tables(){
               
        try {
            Connection con=connect();
            Statement stmt;
            stmt = con.createStatement();
            stmt.executeUpdate("set foreign_key_checks=0");
            stmt.executeUpdate("drop table user_category,libuser,book_category,book,copy,borrow,authorship");  
            stmt.executeUpdate("set foreign_key_checks=1");
            System.out.println("Processing. . .Done. Database is removed.\n"); 
        } catch (SQLException ex) {
            System.out.println("[Error]: "+ex.getMessage());
        }  
    }
    
    //-------------------------------------Read file and deploy data on database--------------------------------------------------
   
    public static void save_user_category() {
         try (Connection con=connect();
             PreparedStatement pstat=con.prepareStatement("INSERT INTO user_category VALUES(?, ?, ?)")){
             
             pstat.setInt(1,ucid);
             pstat.setInt(2,max);
             pstat.setInt(3,period);
             
             pstat.executeUpdate();
             //System.out.print("Saved user_category.");

             con.close();
        } catch(SQLException e){
            System.out.println("[Error]: "+ e.getMessage());
        }
    }
    
    public static void read_user_category(String pathname) throws FileNotFoundException {
        String line;
        Scanner input = new Scanner(new File(pathname+"/"+"user_category.txt"));

            while(input.hasNextLine()){
                 line = input.nextLine();
               
                 try(Scanner data = new Scanner(line)){
                      if(data.hasNextInt()){
                         ucid= data.nextInt();
                      }
                      if(data.hasNextInt()){
                         max= data.nextInt();
                      }
                      if(data.hasNextInt()){
                         period= data.nextInt();
              
                      }                 
                       save_user_category();
                 }  
            }   
    }
    
    public static void save_user() {
          try (Connection con = connect();
             PreparedStatement pstat = con.prepareStatement("INSERT INTO libuser VALUES(?,?,?,?,?)")) {

            pstat.setString(1, libuid);
            pstat.setString(2, name); 
            pstat.setInt(3, age);
            pstat.setString(4, address);
            pstat.setInt(5, ucid); 
            
            pstat.executeUpdate();
       
        } catch (SQLException e) {
            System.out.println("[Error]: "+e.getMessage());
        }
    }
    
    public static void read_user(String pathname) throws FileNotFoundException {
               
        Scanner input = new Scanner(new File(pathname+"/"+"user.txt"));

            while (input.hasNextLine()) {
                String [] arr;
                String line;
                line = input.nextLine();
                name="";
                address="";
                arr=line.split("\t",0);
                   
                libuid=arr[0];
                name=arr[1];
                age=Integer.parseInt(arr[2]);
                address=arr[3];
                ucid=Integer.parseInt(arr[4]);
                   
                save_user();
                       
            }     
    }
    
    public static void save_book_category(){
      try (Connection con=connect();
             PreparedStatement pstat=con.prepareStatement("INSERT INTO book_category VALUES(?, ?)")){
             
             pstat.setInt(1,bcid);
             pstat.setString(2,bcname);
             
             pstat.executeUpdate();
             con.close();
        } catch(SQLException e){
            System.out.println("[Error]: "+ e.getMessage());
        }
    }
    
    public static void read_book_category(String pathname) throws FileNotFoundException{
        Scanner input = new Scanner(new File(pathname+"/"+"book_category.txt"));

            while(input.hasNextLine()){
                String line; 
                line = input.nextLine();
                bcname="";
         
                 try(Scanner data = new Scanner(line)){
                      if(data.hasNextInt()){
                         bcid= data.nextInt();
                      }
                      while (data.hasNext()){
                         bcname+= data.next()+" ";

                      }
                      bcname=bcname.trim();                     
                      save_book_category();
                 } 
            }
    }     
   
    public static void save_book(){
    
        try (Connection con = connect();
             PreparedStatement pstat = con.prepareStatement("INSERT INTO book VALUES(?,?,?,?,?,?)")) {

            pstat.setString(1, callnum);
            pstat.setString(2, title);
            
            SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy");   
            if (publish_temp.equals("null")){
                pstat.setNull(3,Types.DATE); }
            else
            {   Date parsed = format.parse(publish_temp);     
                java.sql.Date sq_publish = new java.sql.Date(parsed.getTime());
                pstat.setDate(3, sq_publish); 
            }
            
            if (rating_temp.equals("null")){
                pstat.setNull(4,Types.FLOAT);
             }
            else
            {   rating=Float.parseFloat(rating_temp);
                pstat.setFloat(4, rating); 
            }
         
            pstat.setInt(5, tborrowed);
            pstat.setInt(6, bcid);
            pstat.executeUpdate();

        } catch (SQLException e) {
            System.out.println("[Error]: "+ e.getMessage());
        } catch (ParseException ex) {
           System.out.println("[Error]: "+ ex.getMessage());
        }
    }
    
    public static void read_book(String pathname) throws ParseException, FileNotFoundException{
            Scanner input = new Scanner(new File(pathname+"/"+"book.txt"));
            while(input.hasNextLine()){
                String line;
                line = input.nextLine();
                String[] arr = line.split("\t",0);
                callnum = arr[0];
                title = arr[2];
        
                publish_temp = arr[4];
             
                rating_temp= arr[5];
                // System.out.println("rating\t");
                tborrowed = Integer.parseInt(arr[6]);
                // System.out.println("tborrowed\t");
                bcid = Integer.parseInt(arr[7]);
                save_book();
            }
         
    }
    
    public static void save_copy(int total_copies){
             int i=1;
        try (Connection con = connect();
             PreparedStatement pstat = con.prepareStatement("INSERT INTO copy VALUES(?,?)")) {

              while (i<=total_copies){ 
                pstat.setString(1, callnum);
                pstat.setInt(2,i);
                i++;
                pstat.executeUpdate();
              }
        } catch (SQLException e) {
              System.out.println("[Error]: "+e.getMessage());
        }
    }
    
    public static void read_copy(String pathname) throws FileNotFoundException{
         int total_copies=0;
         Scanner input = new Scanner(new File(pathname+"/"+"book.txt"));
            while(input.hasNextLine()){
                String line;
                line = input.nextLine();
                String[] arr = line.split("\t",0);
                callnum = arr[0];
                total_copies= Integer.parseInt(arr[1]);
                save_copy(total_copies);  
            }
        
    }
    
    public static void save_borrow() {
        
            try (Connection con = connect();
                PreparedStatement pstat = con.prepareStatement("INSERT INTO borrow VALUES(?,?,?,?,?)")) {

                pstat.setString(1, callnum);
                pstat.setInt(2, copynum); 
                pstat.setString(3, libuid);
                SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy");
                Date parsed = format.parse(checkout_temp);     
                java.sql.Date sq_checkout = new java.sql.Date(parsed.getTime());
                pstat.setDate(4, sq_checkout);
           
                if (returndate_temp.equals("null")){
                    pstat.setNull(5,Types.DATE); 
                }
                else {        
                    Date parsed2 = format.parse(returndate_temp);     
                    java.sql.Date sq_return = new java.sql.Date(parsed2.getTime());
                    pstat.setDate(5, sq_return); 
                }
                pstat.executeUpdate();
            //    System.out.println("Saved borrow.");


            } catch (SQLException e) {
                  System.out.println("[Error]: "+e.getMessage());
            } catch (ParseException e) {
              System.out.println("[Error]: "+e.getMessage());
        }
    }
    
    public static void read_borrow(String pathname) throws ParseException, FileNotFoundException{
            Scanner input = new Scanner(new File(pathname+"/"+"check_out.txt"));
            while(input.hasNextLine()){
                String line;
                line = input.nextLine();
                String[] arr = line.split("\t",0);
                callnum = arr[0];
                copynum = Integer.parseInt(arr[1]);
                libuid = arr[2];
                checkout_temp=arr[3];
                returndate_temp=arr[4];
                                  
                save_borrow();
            } 
        
    }
   
    public static void save_authorship(String[]arr2){
    
        try (Connection con = connect();
            PreparedStatement pstat = con.prepareStatement("INSERT INTO authorship VALUES(?,?)")) {
            for (String a : arr2){
                    pstat.setString(1, callnum); 
                    pstat.setString(2, a);
                    pstat.executeUpdate();
            }
            
        } catch (SQLException e) {
              System.out.println("[Error]: "+e.getMessage());
        }
    }
    
    public static void read_authorship(String pathname) throws FileNotFoundException{
           Scanner input = new Scanner(new File(pathname+"/"+"book.txt"));
            while(input.hasNextLine()){
                String line;
                line = input.nextLine();
                String[] arr = line.split("\t",0);
                aname = arr[3];
                callnum = arr[0];
                String[] arr2=aname.split(",",0);
                save_authorship(arr2);                             
            }   
    }
        
    public static void read_data(String pathname) {
    
        try {
            read_user_category(pathname);
            read_user(pathname);
            read_book_category(pathname);
            read_book(pathname);
            read_copy(pathname);
            read_borrow(pathname);
            read_authorship(pathname);
            System.out.println("Processing. . .Done. Data is inputted to the database.\n");
        } catch (ParseException ex) {
            System.out.println("[Error]: "+ex.getMessage());
        } catch (FileNotFoundException e){
            System.out.println("[Error]: No such file directory");
        }
    }
    
     //------------------------------------------------RECORD COUNT---------------------------------------------------------------------//
    
    public static void count1_authorship(){
       
        try {
            Connection con = connect();
            Statement stmt;
            stmt = con.createStatement();
            String q1 = "select count(*) from authorship";
            ResultSet rs = stmt.executeQuery(q1);
            rs.next();
            int count1 = rs.getInt(1);
            System.out.println("Table1: "+count1);
        
        } catch (SQLException ex) {
           System.out.println("[Error]: "+ex.getMessage());
        }
       
    }
    public static void count2_borrow(){
        try{
            Connection con = connect();
            Statement stmt = con.createStatement();
            String q1 = "select count(*) from borrow"; //check table
            ResultSet rs = stmt.executeQuery(q1);
            rs.next();
            int count2 = rs.getInt(1);
            System.out.println("Table2: "+count2);
        } catch (SQLException ex) {
           System.out.println("[Error]: "+ex.getMessage());
        }
        
    }
    public static void count3_book(){
        try{
            Connection con = connect();
            Statement stmt = con.createStatement();
            String q3 = "select count(*) from book"; //check table
            ResultSet rs = stmt.executeQuery(q3);
            rs.next();
            int count3 = rs.getInt(1);
            System.out.println("Table3: "+count3);
        } catch (SQLException ex) {
           System.out.println("[Error]: "+ex.getMessage());
        }
    }
    public static void count4_user_category(){
        try{
            Connection con = connect();
            Statement stmt = con.createStatement();
            String q4 = "select count(*) from user_category"; //check table
            ResultSet rs = stmt.executeQuery(q4);
            rs.next();
            int count4 = rs.getInt(1);
            System.out.println("Table4: "+count4);
        } catch (SQLException ex) {
           System.out.println("[Error]: "+ex.getMessage());
        }
    } 
    public static void count5_book_category(){
        try{
            Connection con = connect();
            Statement stmt = con.createStatement();
            String q5 = "select count(*) from book_category"; //check table
            ResultSet rs = stmt.executeQuery(q5);
            rs.next();
            int count5 = rs.getInt(1);
            System.out.println("Table5: "+count5);
        } catch (SQLException ex) {
           System.out.println("[Error]: "+ex.getMessage());
        }
    } 
    public static void count6_libuser(){
        try{
            Connection con = connect();
            Statement stmt = con.createStatement();
            String q6 = "select count(*) from libuser"; //check table
            ResultSet rs = stmt.executeQuery(q6);
            rs.next();
            int count6 = rs.getInt(1);
            System.out.println("Table6: "+count6);
        } catch (SQLException ex) {
           System.out.println("[Error]: "+ex.getMessage());
        }
    } 
    public static void count7_copy(){
        try{
            Connection con = connect();
            Statement stmt = con.createStatement();
            String q7 = "select count(*) from copy"; //check table
            ResultSet rs = stmt.executeQuery(q7);
            rs.next();
            int count7 = rs.getInt(1);
            System.out.println("Table7: "+count7);
        } catch (SQLException ex) {
           System.out.println("[Error]: "+ex.getMessage());
        }
    }
    
    public static void count_records() throws SQLException{
        count1_authorship();
        count2_borrow();
        count3_book();
        count4_user_category();
        count5_book_category();
        count6_libuser();
        count7_copy(); 
    
    }
    
          
    public static void Administrator() {
       
        String choice = "0";
        while (choice!="5"){
            System.out.println();
            System.out.println("- - - - - Operations for administrator menu - - - - -\n"
                     + "What kind of Operations would you like to perform?\n"
                + "1. Create all tables\n"
                + "2. Delete all tables\n"
                + "3. Load from datafile\n"
                + "4. Show number of records in each table\n"
                + "5. Return to main menu\n");
            System.out.print("Enter Your Choice:");
            Scanner sc=new Scanner(System.in);
            String adminMenuChoice = sc.next();
            switch (adminMenuChoice){
                case "1":
                    create_table();
                    //System.out.println("Processing. . .Done. Database is initialized.\n");
                    break;
                case "2":                      
                     delete_tables();       
                     break;
                case "3":
                    System.out.print("Type in the Source Data Folder Path: ");
                    String pathname = sc.next();           
                    read_data(pathname); 
                    
                    break;

                case "4":
                    System.out.println("Shows number of records in each table!");
                 {
                     try {
                         count_records();
                     } catch (SQLException ex) {
                        System.out.println("[Error]: "+ex.getMessage());
                     }
                 }
                    break;

                case "5":
                    System.out.println("Returning to main menu\n");
                    Mainmenu();
                default:
                    System.out.println("[Error]: That was not a choice!\n");
            }
        }
    }
    // -----------------------------------------SEARCHING FOR LIBRARY USER ----------------------------------------------------------//
    
    public static int available_copies(String t_callnum) throws SQLException {
        
        Connection con= connect();
        int total_copies=0; int t_nullcount=0; int available_copies=0;
     //   boolean found=false;//------------------------------------------------------NEEED TO DO SOMETHING WITH THISSSSSSSSSSS
        String psql2="select count(*) as total from copy c where c.callnum=?;";
        PreparedStatement pstmt2 = con.prepareStatement(psql2);
        pstmt2.setString(1, t_callnum);
        ResultSet rs2=pstmt2.executeQuery();
        rs2.next();
        total_copies=rs2.getInt("total");
                      
         //counting null return dates corresponding to the callnum 
         String psql="select count(*) as nullcount from borrow b where b.callnum=? and b.returndate is null;";
         PreparedStatement pstmt = con.prepareStatement(psql);
         pstmt.setString(1, t_callnum);
         ResultSet rs=pstmt.executeQuery();
         rs.next();
         t_nullcount=rs.getInt("nullcount");
         available_copies=total_copies-t_nullcount;
         //System.out.println("Number of available copies: "+ available_copies);
         return available_copies;
    }
    
    public static void search1() throws SQLException{
      
        try {
        Connection con= connect();
        Scanner sc=new Scanner(System.in);
        System.out.print("Type in the Search Keyword: ");
        String inp = sc.next(); 
     
        String psql = "SELECT B.callnum,B.title, C.bcname, B.rating FROM  book B, book_category C WHERE B.callnum=? AND B.bcid = C.bcid ORDER BY callnum;";
        PreparedStatement pstmt = con.prepareStatement(psql);
        pstmt.setString(1, inp);
        ResultSet rs = pstmt.executeQuery();
        String count_psql = "SELECT count(*) as count FROM  book B, book_category C WHERE B.callnum=? AND B.bcid = C.bcid;";
        PreparedStatement pstmt3= con.prepareStatement(count_psql);
        pstmt3.setString(1, inp);
        ResultSet rs3=pstmt3.executeQuery();
        rs3.next();
        if ("0".equals(rs3.getString("count"))){
            System.out.println("The book corresponding to this call number does not exist.");
        }
        else {
            System.out.println("|Call Num|Title|Book Category|Author|Rating|Available No. of Copy|");
            String t_aname=""; String t_callnum="";
            int l;
            while(rs.next()){
                t_callnum= rs.getString("callnum");
                System.out.print("|"+rs.getString("callnum")+"|");
                System.out.print(rs.getString("title")+"|");
                System.out.print(rs.getString("bcname")+"|");

                String psql_auth=("select distinct aname from authorship a, book b where a.callnum=? and a.callnum=b.callnum;");
                PreparedStatement pstmt2 = con.prepareStatement(psql_auth);
                pstmt2.setString(1,rs.getString("callnum"));
                ResultSet rs2=pstmt2.executeQuery();
                while (rs2.next()){
                   t_aname+=rs2.getString("aname")+", ";
                }
                l=t_aname.length();
                System.out.print(t_aname.substring(0, l-2)+"|");
                t_aname="";
                System.out.print(rs.getString("rating")+"|");            
                int available_copies=available_copies(t_callnum);
                System.out.println(available_copies+"|"); 

            }  
        }
        } catch (SQLException ex) {
                System.out.println("[Error]: "+ex.getMessage());
            }
    }
    public static void search2() throws SQLException{
        try{
            Connection con= connect();
            Scanner sc=new Scanner(System.in);
            System.out.print("Type in the Search Keyword: ");
            String inp = sc.next(); 
            String psql = "SELECT B.callnum,B.title, C.bcname, B.rating FROM  book B, book_category C WHERE B.title LIKE ? AND B.bcid = C.bcid ORDER BY callnum;";
            PreparedStatement pstmt = con.prepareStatement(psql);
            pstmt.setString(1,"%"+inp+"%");
            ResultSet rs = pstmt.executeQuery();
            String count_psql = "SELECT count(*) as count FROM  book B, book_category C WHERE B.title LIKE ? AND B.bcid = C.bcid;";
            PreparedStatement pstmt3= con.prepareStatement(count_psql);
            pstmt3.setString(1,"%"+inp+"%");
            ResultSet rs3=pstmt3.executeQuery();
            rs3.next();
            if ("0".equals(rs3.getString("count"))){
                System.out.println("The book corresponding to this title does not exist.");
            }
            else{
                System.out.println("|Call Num|Title|Book Category|Author|Rating|Available No. of Copy|");
                String t_aname=""; String t_callnum="";
                int l;
                while(rs.next()){
                    t_callnum= rs.getString("callnum");
                    System.out.print("|"+rs.getString("callnum")+"|");
                    System.out.print(rs.getString("title")+"|");
                    System.out.print(rs.getString("bcname")+"|");

                    String psql_auth=("select distinct  aname from authorship a, book b where a.callnum=? and a.callnum=b.callnum;");
                    PreparedStatement pstmt2 = con.prepareStatement(psql_auth);
                    pstmt2.setString(1,rs.getString("callnum"));
                    ResultSet rs2=pstmt2.executeQuery();
                    while (rs2.next()){
                       t_aname+=rs2.getString("aname")+", ";
                    }
                    l=t_aname.length();
                    System.out.print(t_aname.substring(0, l-2)+"|");
                    t_aname="";


                    System.out.print(rs.getString("rating")+"|");            
                    int available_copies=available_copies(t_callnum);
                    System.out.println(available_copies+"|");

                }
            }
            } catch (SQLException ex) {
            System.out.println("[Error]: "+ex.getMessage());
        }
    }
    public static void search3() throws SQLException{
        try{
            Connection con= connect();
            Scanner sc=new Scanner(System.in);
            System.out.print("Type in the Search Keyword: ");
            String inp = sc.next(); 

            String psql = "SELECT B.callnum,B.title, C.bcname, B.rating FROM  authorship A, book B, book_category C WHERE A.aname like ? AND B.bcid = C.bcid AND A.callnum=B.callnum ORDER BY callnum;";
            PreparedStatement pstmt = con.prepareStatement(psql);
            pstmt.setString(1,"%"+inp+"%");
            ResultSet rs = pstmt.executeQuery();
            String count_psql = "SELECT count(*) as count FROM  authorship A, book B, book_category C WHERE A.aname like ? AND B.bcid = C.bcid AND A.callnum=B.callnum;";
            PreparedStatement pstmt3= con.prepareStatement(count_psql);
            pstmt3.setString(1, "%"+inp+"%");
            ResultSet rs3=pstmt3.executeQuery();
            rs3.next();
            if ("0".equals(rs3.getString("count"))){
                System.out.println("The book corresponding to this author does not exist.");
            }
            else{
                System.out.println("|Call Num|Title|Book Category|Author|Rating|Available No. of Copy|");
                String t_aname=""; String t_callnum="";
                int l;
                while(rs.next()){
                    t_callnum= rs.getString("callnum");
                    System.out.print("|"+rs.getString("callnum")+"|");
                    System.out.print(rs.getString("title")+"|");
                    System.out.print(rs.getString("bcname")+"|");

                    String psql_auth=("select distinct  aname from authorship a, book b where a.callnum=? and a.callnum=b.callnum;");
                    PreparedStatement pstmt2 = con.prepareStatement(psql_auth);
                    pstmt2.setString(1,rs.getString("callnum"));
                    ResultSet rs2=pstmt2.executeQuery();
                    while (rs2.next()){
                       t_aname+=rs2.getString("aname")+", ";
                    }
                    l=t_aname.length();
                    System.out.print(t_aname.substring(0, l-2)+"|");
                    t_aname="";
                    System.out.print(rs.getString("rating")+"|");            
                    int available_copies=available_copies(t_callnum);
                    System.out.println(available_copies+"|");

                }
            }
            } catch (SQLException ex) {
            System.out.println("[Error]: "+ex.getMessage());
        }
    }
    public static void search(){
        
        System.out.println("Choose the Search criterion:\n"
                + "1. call number\n"
                + "2. title\n"
                + "3. author\n");
        Scanner scan = new Scanner(System.in);
        System.out.print("Enter Your Search Criterion: ");
        String schoice = scan.next();
        switch (schoice){
                case "1":
            {
                try {
                    search1();
                } catch (SQLException ex) {
                    System.out.println("[Error]: "+ex.getMessage());
                }
            }
                    System.out.println("End of Query\n");
                    break;

                case "2":
            {
                try {
                    search2();
                } catch (SQLException ex) {
                    System.out.print("[Error]:"+ex.getMessage());
                }
            }
                    System.out.println("End of Query\n");
                    break;

                case "3":
            {
                try {
                    search3();
                } catch (SQLException ex) {
                    System.out.println("[Error]: "+ex.getMessage());
                }
            }
                    System.out.println("End of Query\n");
                    break;

                default:
                    System.out.println("[Error]: That was not a choice!\n");
            }
        
        
    }
    
    //----------------------------------SHOW ALL CHECKOUT RECORDS OF A LIBUSER-----------------------------------------------------------------------
    
    public static boolean isStringNull(String str){
        if (str == null)
            return true;
        else
            return false;
    }
    public static void checkout_libuser() throws SQLException{
        Connection con= connect();
        String t_callnum=" "; String t_copynum=" "; String t_title=" "; 
        String t_aname=""; String t_checkout=" "; String t_returndate=" ";
        Scanner sc=new Scanner(System.in);
        System.out.print("Enter The User ID: ");
        String inp= sc.next();
        String psql=("select br.callnum, br.copynum, b.title, br.checkout, br.returndate from book b, borrow br where br.libuid=? AND b.callnum=br.callnum order by br.checkout DESC;");
        PreparedStatement pstmt = con.prepareStatement(psql);
        pstmt.setString(1, inp);
        ResultSet rs=pstmt.executeQuery();
        String count_psql="select count(*) as count from book b, borrow br where br.libuid=? AND b.callnum=br.callnum order by br.checkout DESC;";
        PreparedStatement pstmt3=con.prepareStatement(count_psql);
        pstmt3.setString(1, inp);
        ResultSet rs3= pstmt3.executeQuery();
        rs3.next();
        if ("0".equals(rs3.getString("count")))
            System.out.println("This user does not exist.");
        else{
            int l;
            System.out.println("|CallNum|CopyNum|Title|Author|Check-out|Returned?|");
            while (rs.next()){
               t_callnum=rs.getString("callnum");
               available_copies(t_callnum);
               System.out.print("|"+rs.getString("callnum")+"|");
               System.out.print(rs.getString("copynum")+"|");
               System.out.print(rs.getString("title")+"|");

               String psql_auth=("select distinct  aname from authorship a, borrow b where a.callnum=? and a.callnum=b.callnum;");
               PreparedStatement pstmt2 = con.prepareStatement(psql_auth);
               pstmt2.setString(1,rs.getString("callnum"));
               ResultSet rs2=pstmt2.executeQuery();
               while (rs2.next()){
                  t_aname+=rs2.getString("aname")+", ";
               }
               l=t_aname.length();
               System.out.print(t_aname.substring(0, l-2)+"|");
               t_aname="";
               System.out.print(rs.getString("checkout")+"|");

               if (isStringNull(rs.getString("returndate"))){ 
                   System.out.println("No|");}
               else 
                   System.out.println("Yes|");           
            }
        }
    }
    
    public static void libraryuser() {
       
        String choice = "0";
        while (choice!="3"){
            System.out.println();
            System.out.println("- - - - - Operations for library user menu - - - - -\n"
                     + "What kind of Operations would you like to perform?\n"
                + "1. Search for Books\n"
                + "2. Show loan record of a user\n"
                + "3. Return to main menu\n");
            System.out.print("Enter Your Choice:");
            Scanner sc=new Scanner(System.in);
            String libuserchoice = sc.next();
            switch (libuserchoice){
                case "1":
                    search();
                    break;
                case "2":
                {
                    try {
                        checkout_libuser();
                    } catch (SQLException ex) {
                       System.out.println("[Error]: "+ex.getMessage());
                    }
                }
                    System.out.println("End of query\n");
                    break;

                case "3":
                    Mainmenu();
                    break;
                default:
                    System.out.println("[Error]: That was not a choice!\n");
            }
        }
    }
    //-------------------------------------------Start of The librarian--------------------------------------------------------------
    //-------------------------------------------Start of Book Borrowing-------------------------------------------------------------
    public static boolean bookcopy_availablity(String call_number,int copy_nunber){
        //Check whether that book copy is available to be borrowed
        
        String sql= "SELECT callnum FROM borrow where callnum='%s' and copynum=%d and returndate is NULL";
        sql=String.format(sql,call_number, copy_nunber);


        try(Connection conn=connect();
            PreparedStatement pstat=conn.prepareStatement(sql);
            ResultSet rs=pstat.executeQuery(sql);){
                if(!rs.isBeforeFirst()){
                    return true;
                }
                if(rs.isBeforeFirst()){
                System.out.println("This book is borrowed by other users.");
                    return false;
                }

            }catch(SQLException e){
                System.out.println("[Error]: "+e.getMessage());
            }

        
        return false;
    }
    public static void mark_record(String call_number, int copy_nunber,String userid){
        //Add a new check-out record of the specified book copy and user with NULL return date  to the database.
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy"); 
        String psql= "insert into borrow(callnum,copynum,libuid,checkout,returndate)"+" values('%s',%d,'%s',CURRENT_TIMESTAMP(),NULL)";
        psql=String.format(psql,call_number,copy_nunber,userid);

     try(Connection conn=connect();
            PreparedStatement pstmt=conn.prepareStatement(psql);
            
              ){  
            pstmt.executeUpdate();  
            System.out.println("Book borrowing performed successfully.");
            }catch(SQLException e){
                System.out.println("[Error]: "+e.getMessage());
            }

    }
    public static void Book_Borrowing(){
        //Book Borrowing operation of the librarian
        System.out.print("Enter The User ID: ");
        Scanner scan = new Scanner(System.in);
        String userid=scan.nextLine();
        System.out.print("Enter The Call Number: ");
        String call_number=scan.nextLine();
        System.out.print("Enter The Copy Number: ");
        int copy_nunber=scan.nextInt();
        if (!(user_exist(userid)&&book_exist(call_number)&&copy_exist(call_number,copy_nunber)))
        librarian();
        if(bookcopy_availablity(call_number, copy_nunber))
            mark_record(call_number,copy_nunber,userid);

        librarian();
        
    }   
    //-------------------------------------------Start of Book Returning-------------------------------------------------------------
    public static boolean return_availablity(String call_number,int copy_nunber,String userid){
        String sql= "SELECT libuid FROM borrow where callnum='%s' and copynum=%d and returndate is NULL";
        sql=String.format(sql,call_number, copy_nunber);
        try(Connection conn=connect();
            PreparedStatement pstat=conn.prepareStatement(sql);
            ResultSet rs=pstat.executeQuery(sql);){
            rs.next();
            if(rs.getString("libuid").equals(userid))
            return true;
            else {
                System.out.println("This book is not borrowed by this user.");
                return false;
            }
            }catch(SQLException e){
                System.out.println("[Error]: "+e.getMessage());
                return false;
            }

    }public static void Return_and_Rate(String call_number,int copy_nunber,double new_rating){
    
        String sql= "SELECT rating,tborrowed FROM book WHERE callnum='%s'";
        String rate="UPDATE book SET rating=%f, tborrowed=%d WHERE callnum='%s'"; 
        String record="UPDATE borrow SET returndate=CURRENT_TIMESTAMP() WHERE callnum='%s' AND copynum=%d AND returndate is NULL";
        sql=String.format(sql, call_number);
        try(Connection conn=connect();
            PreparedStatement pstat=conn.prepareStatement(sql);
            PreparedStatement prate=conn.prepareStatement(rate);
            PreparedStatement precord=conn.prepareStatement(record);
            ResultSet rs=pstat.executeQuery(sql);)
            
            {   rs.next();
                double rating=rs.getDouble("rating");
                int tborrowed=rs.getInt("tborrowed");
                rating=(rating*tborrowed+new_rating)/(tborrowed+1);
                tborrowed++;
                rate=String.format(rate,rating,tborrowed,call_number);
                prate.executeUpdate(rate);
                record=String.format(record,call_number,copy_nunber);
                precord.executeUpdate(record);
                System.out.println("Book returning performed successfully.");
                

            }catch(SQLException e){
                System.out.println("[Error]: "+e.getMessage());
            }
    }
    public static void Book_Returning(){
        //Book Returning operation of the librarian
        System.out.print("Enter The User ID: ");
        Scanner scan = new Scanner(System.in);
        String userid=scan.nextLine();
        System.out.print("Enter The Call Number: ");
        String call_number=scan.nextLine();
        System.out.print("Enter The Copy Number: ");
        int copy_nunber=scan.nextInt();
        System.out.print("Enter Your Rating of the BOOK: ");
        double new_rating=scan.nextDouble();
        if (!(user_exist(userid)&&book_exist(call_number)&&copy_exist(call_number,copy_nunber)))
        librarian();
        if(return_availablity(call_number, copy_nunber, userid)){
            Return_and_Rate(call_number, copy_nunber,new_rating);
        }
        librarian();
    }
    //-------------------------------------------Start of List All-------------------------------------------------------------
    public static void List_all (){
        //List all un-returned book copies which are checked-out within a period operation of the librarian
          System.out.print("Type in the starting date[dd/mm/yyyy]: ");
          Scanner scan = new Scanner(System.in);
          String start_datestring=scan.nextLine();
          SimpleDateFormat formatter= new SimpleDateFormat("dd/MM/yyyy");
          SimpleDateFormat formatter_forprint= new SimpleDateFormat("yyyy-MM-dd");
          System.out.print("Type in the ending date[dd/mm/yyyy]: ");
          String end_datestring=scan.nextLine();
          String sql= "SELECT * FROM borrow WHERE returndate is NULL ORDER BY Checkout DESC";
          System.out.println("List of Unreturned Book: ");
          System.out.println("|LibUID|CallNUM|CopyNum|Checkout|");
  //        Date starting_date=formatter.format(datestring);
          try(Connection conn=connect();
          PreparedStatement pstat=conn.prepareStatement(sql);
          ResultSet rs=pstat.executeQuery(sql);){
  
          Date starting_date=formatter.parse(start_datestring);
          Date ending_date=formatter.parse(end_datestring);
          while(rs.next()){
          if(!rs.getDate("checkout").before(starting_date) && !rs.getDate("checkout").after(ending_date)){
          String uid=rs.getString("libuid");
          String callnum=rs.getString("callnum");
          String copynum=rs.getString("copynum");
          Date checkout_date=rs.getDate("checkout");
          String checkout_date_string=formatter_forprint.format(checkout_date);
          System.out.println("|"+uid+"|"+callnum+"|"+copynum+"|"+checkout_date_string+"|");
          }
          }
  
          }catch(Exception e){
              System.out.println("[Error]: "+e.getMessage());
          }
          System.out.println("End of Query");
          librarian();
        }
    public static void librarian(){
        String choice = "0";
        while (choice!="4"){
            System.out.println("What kind of operation would you like to perform?\n"
                + "1. Book borrowing\n"
                + "2. Book returning\n"
                + "3. List all the unreturned book copies which are checked-out within a period\n"
                + "4. Return to Main menu\n");
            Scanner scan = new Scanner(System.in);
            System.out.print("Enter Your Choice:");
            choice = scan.next();
            switch (choice){
                case "1":
                    Book_Borrowing();
                    break;
                case "2":
                     Book_Returning();
                    break;
                case "3":
                    List_all();
                    break;
                case "4":
                    Mainmenu();
                    break;
                default:
                    System.out.println("[Error]: That was not a correct choice!\n");
            }
        }
    }
    //---------------------------------------This is the start of check inout
    public static boolean user_exist(String userid){
        //return 1 if the user exist in our library
        String sql= "SELECT libuid FROM libuser WHERE libuid='%s'";
        sql=String.format(sql,userid);
        try(Connection conn=connect();
        PreparedStatement pstat=conn.prepareStatement(sql);
        ResultSet rs=pstat.executeQuery(sql);){
            if(!rs.isBeforeFirst()){
            System.out.println("Library don't have information of  this user\n");
            return false;
            }
            return true;
            }catch(Exception e){
                System.out.println("[Error]: "+e.getMessage());
            return false;
            }
    }
    
    public static boolean book_exist(String call_num){
        String sql= "SELECT callnum FROM book WHERE callnum='%s'";
        sql=String.format(sql,call_num);
        try(Connection conn=connect();
        PreparedStatement pstat=conn.prepareStatement(sql);
        ResultSet rs=pstat.executeQuery(sql);){
            if(!rs.isBeforeFirst()){
            System.out.println("Library don't have this book.");
            return false;
            }
            return true;
        }catch(Exception e){
            System.out.println("[Error]: "+e.getMessage());
        return false;
        }
    }
    public static boolean copy_exist(String call_num, int copy_num){
        String sql= "SELECT * FROM copy WHERE callnum='%s' AND copynum=%d";
        sql=String.format(sql,call_num,copy_num);
        try(Connection conn=connect();
        PreparedStatement pstat=conn.prepareStatement(sql);
        ResultSet rs=pstat.executeQuery(sql);){
            if(!rs.isBeforeFirst()){
            System.out.println("Library don't have this book copy");
            return false;
            }
            return true;
        }catch(Exception e){
            System.out.println("[Error]: "+e.getMessage());
            return false;
        }
    }
    //-----------------------------End of Librarian-----------------------------------------
    public static void Mainmenu(){
        String choice = "0";
        while (choice!="4"){
            System.out.println();
            System.out.println("- - - - - Main menu - - - - -\n"
                    + "What kinds of operations would you like to perform?\n"
                    + "1. Operations for Administrator\n"
                    + "2. Operations for Library User\n"
                    + "3. Operations for Librarian\n"
                    + "4. Exit this program\n");
            Scanner scan = new Scanner(System.in);
            System.out.print("Enter Your Choice:"); 
            choice = scan.next();
            switch (choice){
                case "1":
                    Administrator();
                    break;
                case "2":
                    libraryuser();
                    break;
                case "3":
                    librarian();
                    break;
                case "4":
                    System.out.println("Thank you for your time! Bye!");
                    System.exit(0);
                    break;
                default:
                    System.out.println("[Error]: That was not a correct choice!\n");
            }
        }
    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        System.out.println("Welcome to Library Inquiry System!\n");
        Mainmenu();

    }
}