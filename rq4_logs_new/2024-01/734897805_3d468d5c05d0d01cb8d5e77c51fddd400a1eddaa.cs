public class Employee
{
    public string Name { get; set; }
    public string Position { get; set; }
    public DateTime SeparationDate { get; set; }
}

class Program
{
    static void Main()
    {
        List<Employee> employees = new List<Employee>
        {
            new Employee { Name = "John Smith", Position = "Manager", SeparationDate = DateTime.Parse("2022-01-27") },
            new Employee { Name = "Tou Xiong", Position = "Developer", SeparationDate = DateTime.Parse("2023-05-15") },
            new Employee { Name = "Michaela Michaelson", Position = "Designer", SeparationDate = DateTime.Parse("2021-12-10") },
            new Employee { Name = "Jake Jacobson", Position = "Analyst", SeparationDate = DateTime.Parse("2024-02-01") },
            new Employee { Name = "Jacquelyn Jackson", Position = "Engineer", SeparationDate = DateTime.Parse("2023-07-20") },
            new Employee { Name = "Sally Weber", Position = "Administrator", SeparationDate = DateTime.Parse("2022-08-05") }
        };
        
        Console.WriteLine("Name                 | Position           | Separation Date");
        Console.WriteLine("---------------------|--------------------|----------------");

        foreach (var employee in employees)
        {
            Console.WriteLine($"{employee.Name,-20} | {employee.Position,-18} | {employee.SeparationDate:yyyy-MM-dd}");
        }
    }
}