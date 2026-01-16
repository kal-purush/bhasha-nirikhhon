using System;
public class Employee
{
    
    public string name;
    public double salaryPerHour;
    public double hoursWorked;
    public double salary;
    public string salaryMessage;
    public Employee (string _name, double _salaryPerHour, double _hoursWorked)
    {
        this.name = _name;
        this.salaryPerHour = _salaryPerHour;
        this.hoursWorked = _hoursWorked;
        this.salaryMessage = "To be paid in standard order";
        if (this.hoursWorked > 60) this.salaryMessage = "Error! More than 60 hours worked!";
        if (this.salaryPerHour < 8) this.salaryMessage = "Error! Not less than 8.00 USD/h are allowed according to US legislation!";
        if (this.hoursWorked <= 40) this.salary = this.hoursWorked * this.salaryPerHour;
        else this.salary = (1.5 * this.hoursWorked - 20) * this.salaryPerHour;
    }
}

namespace Excercise8
{
    class Program
    {
        static void Main(string[] args)
        {
            Employee[] companyFoo = new Employee[3];
            companyFoo[0] = new Employee("Mister Jecabson", 7.50, 35);
            companyFoo[1] = new Employee("Mister Jochanson", 8.20, 47);
            companyFoo[2] = new Employee("Mister Jason", 10, 73);
            Console.WriteLine("| Employee | Base Pay | Hours Worked | Salary | Instructions |");
            for (int i = 0; i < companyFoo.Length; i++)
            {
                Console.WriteLine($"| {companyFoo[i].name} | { companyFoo[i].salaryPerHour.ToString("0.00")} | { companyFoo[i].hoursWorked.ToString("0.00")} | { companyFoo[i].salary.ToString("0.00")} | { companyFoo[i].salaryMessage} |");
            }
                Console.ReadKey();  
           
        }
    }
}