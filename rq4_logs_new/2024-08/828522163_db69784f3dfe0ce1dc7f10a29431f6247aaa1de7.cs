using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlexWheels
{
	internal class Admin
	{
		
        private string adminId;
		private string role;
		private DateTime lastActivityDate;

		public string AdminId
		{
			get { return adminId; }
			set { adminId = value; }
		}

		public string Role
		{
			get { return role; }
			set { role = value; }
		}

		public DateTime LastActivityDate
		{
			get { return lastActivityDate; }
			set { lastActivityDate = value; }
		}

		// Default constructor
		public Admin() { }

		// Parameterized constructor
		public Admin(string adminId, string role, DateTime lastActivityDate)
		{
			this.adminId = adminId;
			this.role = role;
			this.lastActivityDate = lastActivityDate;
		}

		public override string ToString()
		{
			return $"Admin ID: {adminId}, Role: {role}, Last Activity Date: {lastActivityDate.ToShortDateString()}";
		}
	}
}
	
