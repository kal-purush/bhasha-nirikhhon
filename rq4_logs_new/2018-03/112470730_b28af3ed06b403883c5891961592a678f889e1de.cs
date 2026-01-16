using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WpfApp1.Model
{
	public class Consent
	{
		public int UserID { get; set; }
		public int PermissionID { get; set; }
		public DateTime CreatedTime { get; set; }
		public DateTime ExpiredTime { get; set; }
		public string LegalText { get; set; }

		public Consent(int userID, int permissionID, DateTime createdTime, DateTime expiredTime, string legalText)
		{
			UserID = userID;
			PermissionID = permissionID;
			CreatedTime = createdTime;
			ExpiredTime = expiredTime;
			LegalText = legalText;
		}

	}
}