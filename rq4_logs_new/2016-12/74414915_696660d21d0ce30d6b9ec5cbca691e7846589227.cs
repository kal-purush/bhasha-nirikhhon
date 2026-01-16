using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BizWorks
{
	public class UserPositionID 
	{
		private int id;
		private string position;
		private double pay;
		
		//constructor
		public UserPositionID(int userId, string userPosition, double userPay)
		{
			UserId = userId;
			UserPosition = userPosition;
			UserPay = userPay;
		}
		
		public int UserId
		{
			get { return userId; }
			set { userId = value; }
		}
		
		public string UserPosition
		{
			get { return userPosition; }
			set { userPosition = value; }
		}
		
		public double UserPay
		{
			get { return userPay; }
			set { userPay = value; }
		}
	}
}