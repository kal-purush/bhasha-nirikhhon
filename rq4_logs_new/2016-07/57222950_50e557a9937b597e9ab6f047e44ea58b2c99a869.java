public class Employee{
  
  private int id;
  private String name;
  private double percent;
  
 void setid(int i) { this.id=i;}
 void setname(String i) { this.name=i;}
 void setpercent(double i) { this.percent=i;}
 
 public Employee() {}
 public Employee(int i, String n, double p) { this.id=i;this.name=n;this.percent=p; }
 public String toString()
 {
   return "id: " + String.valueOf(id) + ", Name: "+String.valueOf(name) + ", Percent: " + String.valueOf(percent);
 }
}