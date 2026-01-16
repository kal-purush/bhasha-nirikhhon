using Microsoft.AspNetCore.Http;
using System.Threading.Tasks;

namespace APIGateway.Middlewares
{
	internal class HideInternalRoutesMiddleware
	{
		private static readonly string INTENAL_ROUTE_KEY = "Internal";

		private readonly RequestDelegate next;

		public HideInternalRoutesMiddleware(RequestDelegate next)
		{
			this.next = next;
		}

		public async Task Invoke(HttpContext context)
		{
			if (context.Request.Path.Value.Contains(INTENAL_ROUTE_KEY)) return;
			await next.Invoke(context);
		}
	}
}