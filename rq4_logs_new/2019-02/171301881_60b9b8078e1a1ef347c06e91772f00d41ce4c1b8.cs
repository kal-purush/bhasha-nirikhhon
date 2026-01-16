namespace WebApiProject.Models
{
    public class Employee : IEmployee
    {
        public int EmployeeId { get; set ; }
        public string EmployeeName { get; set; }  //this line fixes the issue
        public string Department { get; set; }    //fixing another bug
    }
}