using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GettingReal
{
	public class ConsentDomainController
	{
		//Singleton
		private static ConsentDomainController instance;
		public static ConsentDomainController Instance
		{
			get
			{
				if(instance == null)
				{
					instance = new ConsentDomainController();
				}

				return instance;
			}
		}

		private ConsentDomainController()
		{

		}

		public string SaveConsent(int userID, int permissionID, DateTime expirationTime)
		{
			Dictionary<string, object> parameterInput = new Dictionary<string, object>();

			parameterInput.Add("UserID", userID);
			parameterInput.Add("PermissionID", permissionID);
			parameterInput.Add("ExpirationTime", expirationTime);

			return ConsentDatabaseController.ExecuteNonQuery("SaveConsent", parameterInput);
		}

		public string CheckForConsent(int userID, int permissionID)
		{
			Dictionary<string, object> parameterInput = new Dictionary<string, object>();

			parameterInput.Add("UserID", userID);
			parameterInput.Add("PermissionID", permissionID);

			bool result = false;
			string errorMessage = ConsentDatabaseController.CheckQuery("CheckConsent", parameterInput, out result);

			if(errorMessage != "")
			{
				if(result)
				{
					return "1";
				}
				else
				{
					return "0";
				}
			}

			return errorMessage;
		}

		public string RetrieveAllConsents(int userID)
		{
			Dictionary<string, object> parameterInput = new Dictionary<string, object>();
			parameterInput.Add("UserID", userID);

			List<object[]> table;
			string errorMessage = ConsentDatabaseController.RetrieveQuery("RetrieveAllConsents", parameterInput, out table);

			return DomainSharedMethods.GetTableOrError(errorMessage, table);
		}

		public string RevokeConsent(int userID, int permissionID)
		{
			Dictionary<string, object> parameterInput = new Dictionary<string, object>();

			parameterInput.Add("UserID", userID);
			parameterInput.Add("PermissionID", permissionID);

			return ConsentDatabaseController.ExecuteNonQuery("RevokeConsent", parameterInput);
		}

		public string RetrieveRequestResponse(int userID)
		{
			List<object[]> table = new List<object[]>();

			string path = @"c:\GettingReal\Customers\" + userID;
			string[] allFileData = FileHandler.RetrieveAllFilesInFolder(path);

			if(allFileData != null)
			{
				for(int i = 0; i < allFileData.Length; i++)
				{
					string[] fileValues = allFileData[i].Split(';');
					table.Add(fileValues);
				}
			}

			return DomainSharedMethods.ConvertTableToString(table);
		}

	}
}