//using Microsoft.AspNetCore.Authentication;
//using Microsoft.AspNetCore.Http;
//using Microsoft.Extensions.Logging;
//using Microsoft.Extensions.Options;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Security.Claims;
//using System.Text;
//using System.Text.Encodings.Web;
//using System.Threading.Tasks;

//namespace Nt.Infrastructure.Tests.MockAuthenticationMiddleware
//{
//	public class TestAuthenticationOptions : AuthenticationOptions
//	{
//		public virtual ClaimsIdentity Identity { get; } = new ClaimsIdentity(new Claim[]
//		{
//		new Claim("http://schemas.xmlsoap.org/ws/2005/05/identity/claims/nameidentifier", Guid.NewGuid().ToString()),
//		new Claim("http://schemas.microsoft.com/identity/claims/tenantid", "test"),
//		new Claim("http://schemas.microsoft.com/identity/claims/objectidentifier", Guid.NewGuid().ToString()),
//		new Claim("http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname", "test"),
//		new Claim("http://schemas.xmlsoap.org/ws/2005/05/identity/claims/surname", "test"),
//		new Claim("http://schemas.xmlsoap.org/ws/2005/05/identity/claims/upn", "test"),
//		}, "test");
//        public string AuthenticationScheme { get; }
//        public bool AutomaticAuthenticate { get; }

//        public TestAuthenticationOptions()
//		{
//			AuthenticationScheme = "TestAuthenticationMiddleware";
//			AutomaticAuthenticate = true;
//		}
//	}

//	public class TestAuthenticationHandler : AuthenticationHandler<TestAuthenticationOptions>
//	{
//		protected override Task<AuthenticateResult> HandleAuthenticateAsync()
//		{
//			var authenticationTicket = new AuthenticationTicket(
//											 new ClaimsPrincipal(Options.Identity),
//											 new AuthenticationProperties(),
//											 this.Options.AuthenticationScheme);

//			return Task.FromResult(AuthenticateResult.Success(authenticationTicket));
//		}
//	}

//	public class TestAuthenticationMiddleware : AuthenticationMiddleware<TestAuthenticationOptions>
//	{
//		private readonly RequestDelegate next;

//		public TestAuthenticationMiddleware(RequestDelegate next, IOptions<TestAuthenticationOptions> options, ILoggerFactory loggerFactory)
//			: base(next, options, loggerFactory, UrlEncoder.Default)
//		{
//			this.next = next;
//		}

//		protected override AuthenticationHandler<TestAuthenticationOptions> CreateHandler()
//		{
//			return new TestAuthenticationHandler();
//		}
//	}
//}