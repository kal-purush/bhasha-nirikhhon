import java.util.ArrayList;

class Employee{
    
    private int empid;
    private String empName;
    private String empDesignation;
    private int empSalary;
    private String empLocation;
    
    public Employee(int empid, String empName,String empDesignation,int empSalary,String empLocation){
        this.empid=empid;
        this.empName=empName;
        this.empDesignation=empDesignation;
        this.empSalary=empSalary;
        this.empLocation=empLocation;
    }
    
    public void setEmpid(int empId){
        empid=empId;
    }
    
    public int getEmpid(){
        return empid;
    }
    
    
    public String getEmpName(){
        return empName;
    }
    
    
    public String getEmpDesignation(){
        return empDesignation;
    }
    
    
    public int getEmpSalary(){
        return empid;
    }
    
    
    public String getEmpLocation(){
        return empLocation;
    }
}

public class EmployeeDetails{
    public static void main(String args[]){
        Employee e1=new Employee(1,"Roshan","Android Developer",90000,"Hazaribagh");
        Employee e2=new Employee(2,"Abhishek","hacker",70000,"Patna}");
        Employee e3=new Employee(3,"Pranav","Artificial Intelligence",60000,"Jammu");
        Employee e4=new Employee(4,"Aniket","Data Science",50000,"Mumbai");
        Employee e5=new Employee(5,"Vijay","Big Data",65000,"Noida");
        Employee e6=new Employee(6,"Rishabh","Business",45000,"UttarPradesh");
        Employee e7=new Employee(7,"Guru","Entrepreneur",60000,"Jamshedpur");
        
        ArrayList<Employee> employee=new ArrayList<>();
        employee.add(e1);
        employee.add(e2);
        employee.add(e3);
        employee.add(e4);
        employee.add(e5);
        employee.add(e6);
        employee.add(e7);
        
        for (int i=0;i<employee.size();i++)
        {
            System.out.println(employee.get(i).getEmpName());
        }
        
        for (int i=0;i<employee.size();i++)
        {
            if (employee.get(i).getEmpSalary()>50000)
            {
                System.out.println(employee.get(i).getEmpName());
            }
        }
        
        for (int i=0;i<employee.size();i++)
        {
            if (employee.get(i).getEmpLocation().startsWith("M"))
            {
                System.out.println(employee.get(i).getEmpLocation());
            }
        }
        
        for (int i=0;i<employee.size();i++)
        {
            if (employee.get(i).getEmpDesignation().endsWith("e"))
            {
                System.out.println(employee.get(i).getEmpDesignation());
            }
        }
    }
}