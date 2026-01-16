using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Filters;
using PetOwner.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PetOwner.Helpers
{
	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method)]
	public class AuthorizeAttribute : Attribute, IAuthorizationFilter
	{
		public void OnAuthorization(AuthorizationFilterContext context)
		{
			var user = (User)context.HttpContext.Items["User"];

			if(user == null)
			{
				context.Result = new Microsoft.AspNetCore.Mvc.JsonResult(new { Message = "Unauthorized" })
				{
					StatusCode = StatusCodes.Status401Unauthorized
				};
			}
		}
	}
}