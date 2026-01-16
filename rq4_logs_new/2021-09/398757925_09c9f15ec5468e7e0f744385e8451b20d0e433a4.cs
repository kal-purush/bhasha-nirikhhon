using System.Net;

using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;

using Ralfred.Common.Exceptions;
using Ralfred.SecretsProvider.Models;


namespace Ralfred.SecretsProvider
{
	public class ExceptionsFilter : IExceptionFilter
	{
		public void OnException(ExceptionContext context)
		{
			var status = HttpStatusCode.InternalServerError;
			var message = "Something went wrong";

			if (context.Exception is NotFoundException)
			{
				status = HttpStatusCode.NotFound;
				message = context.Exception.Message;
			}

			context.ExceptionHandled = true;
			context.HttpContext.Response.StatusCode = (int) status;
			context.HttpContext.Response.ContentType = "application/json";

			context.Result = new ObjectResult(new Error
			{
				StatusCode = (int) status,
				Message = message
			});
		}
	}
}