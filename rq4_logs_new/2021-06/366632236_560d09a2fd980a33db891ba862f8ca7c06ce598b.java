import java.util.Scanner;
class BookInheritance
{
 public static void main(String args[])
  {
   Scanner sc= new Scanner(System.in);
  System.out.println("how many book details to enter:");
  int count=sc.nextInt();
   book b[]=new book[count];
   for (int i=0; i<count; i++)
   {
    b[i]=new book();
    b[i].read();
    }
    Scanner st= new Scanner(System.in);
     System.out.println("enter the category of book you want to see:");
     String ct=st.nextLine();
     boolean flag=false;
    for (int i=0; i<count; i++)
    {
      if(ct.equals(b[i].category))
       {
             System.out.println(ct+" category book  details:");
             b[i].show(); 
             flag=true;
       }
     else
       {
        continue;
		 }
    }
 if(!flag)
    System.out.println("your searched gategory book is not available:");
  }
}
class publisher
{
 String name;
 String location;
 String ISBN_number;
void read()
{
 Scanner sc= new Scanner(System.in);
 System.out.println(" Enter name of publisher:");
 name=sc.nextLine();
 System.out.println(" Enter the location:");
 location=sc.nextLine();
 System.out.println(" Enter the ISBN number:");
 ISBN_number=sc.nextLine();
}
}
class book extends publisher
{
String author;
String title;
int price;
String category;
void read()
{
 super.read();
 Scanner sc= new Scanner(System.in);
 System.out.println(" Enter name of author name:");
 author=sc.nextLine();
 System.out.println(" Enter the book title:");
 title=sc.nextLine();
 System.out.println(" Enter the category of book:");
 category=sc.nextLine();
 System.out.println(" Enter the price of book:");
 price=sc.nextInt();
}
void show()
{
 System.out.println("The book title is : "+title);
 System.out.println(" The book author name is :"+author);
 System.out.println("the gategory of the book :"+category);
 System.out.println("the bublisher of the book is:"+name);
  System.out.println(" publisher location is :"+location);
 System.out.println("the publisher ISBN number is:"+ISBN_number);
}
}