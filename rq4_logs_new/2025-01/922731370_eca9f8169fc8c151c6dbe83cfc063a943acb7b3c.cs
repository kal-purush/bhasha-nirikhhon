namespace Sample.Api.Middlewares
{
	public class AuthenticatorMiddleware
	{
		private readonly RequestDelegate _next;

		public AuthenticatorMiddleware(RequestDelegate next)
		{
			_next = next;
		}

		public async Task InvokeAsync(HttpContext context)
		{
			if (!context.Request.Headers.TryGetValue("Authorization", out Microsoft.Extensions.Primitives.StringValues value))
			{
				context.Response.StatusCode = StatusCodes.Status401Unauthorized;
				await context.Response.WriteAsync("Authorization token is missing.");
				return;
			}

			var token = value.ToString();

			if (string.IsNullOrWhiteSpace(token) || !token.StartsWith("Bearer "))
			{
				context.Response.StatusCode = StatusCodes.Status401Unauthorized;
				await context.Response.WriteAsync("Invalid or missing Authorization token.");
				return;
			}
			await _next(context);
		}
	}

}