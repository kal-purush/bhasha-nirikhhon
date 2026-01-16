// See https://aka.ms/new-console-template for more information
//Console.WriteLine("Hello, World!");
namespace EmployeeWages
{
    internal class CompanyEmpWage
    {
        internal int wagePerHour;
        private string company;
        private int maxHoursPerMonth;
        private int maxWorkingDays;

        public CompanyEmpWage(string company, int wagePerhour, int maxHoursPerMonth, int maxWorkingDays)
        {
            this.company = company;
            wagePerHour = wagePerhour;
            this.maxHoursPerMonth = maxHoursPerMonth;
            this.maxWorkingDays = maxWorkingDays;
        }

        internal bool toString()
        {
            throw new NotImplementedException();
        }

        internal void setTotalEmpWage(int v)
        {
            throw new NotImplementedException();
        }
    }
}