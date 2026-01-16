using System;

namespace PayrollSystem
{
	public class Employee
	{
		public String EmployeeID
		{
			get
			{
				return
			}

		}
		public String EmployeeName
		{
			get
			{
				return employee_Name;
			}
			set
			{
				employee_Name = value;
			}
			
		}
		public String EmployeeAddress
		{
			get {
				return employee_Address;
			}

			set
			{
				employee_Address = value;
			}
		}


		protected String employee_ID;
		protected String employee_Name;
		protected String employee_Address;
		protected String employee_Paymethod { get; set; }
		protected String employee_Type { get; set; }


	}
}