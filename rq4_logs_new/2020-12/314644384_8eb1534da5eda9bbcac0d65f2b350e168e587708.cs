using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PetOwner.Helpers
{
	public static class Errors
	{
		public  enum ErrorCode
		{
			User_Not_Found,
			Email_Or_Password_Invalid,
			Email_Already_Used,
			User_Already_Has_Vip,
			Insert_Vip_Error,
			User_Doesnt_Have_Vip,
			VipId_Not_Found,
			Group_Not_Found,
			Group_Users_Empty,
			Add_User_To_Group_Error,
			Patch_Group_Name_Failed,
			Group_Delete_Failed,
			Insert_User_To_Group_Failed,
			Insert_CostItem_Failed,
			CostItem_Not_Found,
			Group_Pets_Not_Found,
			Insert_Pet_To_Group_Failed,
			Pet_Update_Failed,
			Pet_Not_Found,
			
		}
	}
}