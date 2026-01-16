using System;
using PayrollSystem;
	public class EmployeeFactory
	{
		public  Employee getEmployee(String employeeType)
		{
			if (employeeType == null)
			{
			return null;
			}

			if (employeeType.Equals("hourly", StringComparison.InvariantCultureIgnoreCase))
			{
				return new Employee_Hourly();
			}
			else if (employeeType.Equals("commission", StringComparison.InvariantCultureIgnoreCase))
			{
				return new CommEmployee();
			}
			else if (employeeType.Equals("hourly", StringComparison.InvariantCultureIgnoreCase))
			{
				return new Employee_Salary();
			}

		return null;
		}

	}
