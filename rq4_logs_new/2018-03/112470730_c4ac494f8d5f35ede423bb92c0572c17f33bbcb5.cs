using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Domain;

namespace Application
{
	internal class Converter
	{

		internal static List<string[]> ConvertToStringArrayList(List<Permission> permissionList)
		{
			if(permissionList == null)
			{
				return null;
			}

			List<string[]> arrayList = new List<string[]>();

			foreach(Permission permission in permissionList)
			{
				string[] stringArray = new string[]
				{
					Convert.ToString(permission.PermissionID),
					permission.LegalText
				};

				arrayList.Add(stringArray);
			}

			return arrayList;
		}

		internal static List<string[]> ConvertToStringArrayList(List<Consent> consentList)
		{
			if(consentList == null)
			{
				return null;
			}

			List<string[]> arrayList = new List<string[]>();

			foreach(Consent consent in consentList)
			{
				string[] stringArray = new string[]
				{
					Convert.ToString(consent.UserID),
					Convert.ToString(consent.PermissionID),
					Convert.ToString(consent.CreatedTime),
					Convert.ToString(consent.ExpirationTime),
					consent.LegalText
				};

				arrayList.Add(stringArray);
			}

			return arrayList;
		}

		internal static List<string[]> ConvertToStringArrayList(List<PermissionRequest> permissionRequestList)
		{
			if(permissionRequestList == null)
			{
				return null;
			}

			List<string[]> arrayList = new List<string[]>();

			foreach(PermissionRequest permissionRequest in permissionRequestList)
			{
				string[] stringArray = new string[]
				{
					Convert.ToString(permissionRequest.UserID),
					Convert.ToString(permissionRequest.PermissionID),
					Convert.ToString(permissionRequest.Duration),
					Convert.ToString(permissionRequest.Response),
					permissionRequest.LegalText
				};

				arrayList.Add(stringArray);
			}

			return arrayList;
		}

	}
}