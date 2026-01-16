import java.util.ArrayList;
import  java.util.*;
abstract class Employee{
//    ----------------use access specifier ------------
    private String name;
    private int id;
//    ----------------- use constructer ------------------
    public Employee(String name,int id){
        this.name = name;
        this.id = id;
    }
//    ------------------------- use incapculation ----------------
    public String getName(){
        return name;
    }
    public int getId(){
        return id;
    }
//    ------------------------ use abstract method ----------------
    public abstract double calculateSalary();
//    -------------------------- use polymorfism -------------------------
    @Override
    public String toString(){
        return "Employee [name = " +name+ ", id = " +id+ ", salary = " +calculateSalary() +"]";
    }
}



//------------------------ inheritence used --------------------
class FullTimeEmployee extends Employee{
    private double monthlySalary;
    public FullTimeEmployee(String name,int id,double monthlySalary){
//        ----------------------- using super class constracter values by super keyword ------------------
        super(name,id);
        this.monthlySalary = monthlySalary;
    }
//    ------------------------ using override method from super class-----------
    @Override
    public double calculateSalary(){
        return monthlySalary;
    }
}
//------------------------
class PartTimeSalary extends Employee{
    private int hoursWorked;
    private double hoursRate;
    public PartTimeSalary(String name, int id, int hoursWorked, double hoursRate){
        super(name, id);
        this.hoursWorked = hoursWorked;
        this.hoursRate = hoursRate;
    }
    @Override
    public double calculateSalary(){
        return hoursWorked*hoursRate;
    }

}
class PayrollSystem{
    private ArrayList<Employee> employeesList;
    public PayrollSystem(){
        employeesList = new ArrayList<>();
    }
    public void addEmployee(Employee employee){
        employeesList.add(employee);
    }
    public void removeEmployee(int id ){
        Employee employeeToRemove = null;
        for(Employee employee : employeesList){
            if(employee.getId() == id){
                employeeToRemove = employee;
                break;
            }
        }
        if(employeeToRemove != null){
            employeesList.remove(employeeToRemove);
        }
    }
    public void displayEmployee(){
        for(Employee employee : employeesList){
            System.out.println(employee);
        }
    }
}
public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        PayrollSystem payroll = new PayrollSystem();

//        System.out.println("which type of employee you want to add: ");
//        System.out.println("press key for fulltime:1 & parttime:2 ");
//        int i = sc.nextInt();

        String name = sc.nextLine();
        int id = sc.nextInt();
        double salary = sc.nextDouble();

        FullTimeEmployee emp1 = new FullTimeEmployee(name,id,salary);
        PartTimeSalary emp2 = new PartTimeSalary("shreya",2,3,100);

        payroll.addEmployee(emp1);
        payroll.addEmployee(emp2);

        System.out.println("Employee detial: ");
        payroll.displayEmployee();

        payroll.removeEmployee(1);
        System.out.println("Remaing Employee detial: ");
        payroll.displayEmployee();

        
    }
}