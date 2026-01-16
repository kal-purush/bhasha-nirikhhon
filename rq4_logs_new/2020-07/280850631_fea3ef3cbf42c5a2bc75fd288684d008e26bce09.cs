using Covid19TemperatureAPI.Entities.Data;
using Covid19TemperatureAPI.Helper;
using IdentityModel.Client;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace Covid19TemperatureAPI.Controllers
{
    [Route("c19server/login")]
    [ApiController]
    public class LoginController: ControllerBase
    {
        private IConfiguration Configuration;
        private readonly ILogger<LoginController> _logger;
        private ApplicationDbContext Context { get; set; }

        private enum ResponseCodes
        {
            [Display(Name = "Successful")]
            Successful = 1200,
            [Display(Name = "Error")]
            SystemError = 1201,
            [Display(Name = "Authentication Failed")]
            AuthenticationFailed = 1202,
        }

        public LoginController(IConfiguration configuration, ApplicationDbContext applicationDbContext, ILogger<LoginController> logger)
        {
            Configuration = configuration;
            Context = applicationDbContext;
            _logger = logger;
        }

        /// <summary>
        /// Login API. Returns a JWT bearer token. The token needs to be passed to access any APIs
        /// </summary>
        /// <remarks>
        /// Sample request:
        /// 
        ///     POST /c19server/login
        ///     {
        ///        "username": "username",
        ///        "password": "password"
        ///     }
        ///      
        /// Sample response:
        /// 
        ///     {
        ///        "respcode" = "1200",
        ///        "description" = "successful",
        ///        "token" = "2bb8dc60....",
        ///     }
        ///     
        /// Response codes:
        ///     1200 = "Successful"
        ///     1201 = "Error"
        ///     1202 = "Authentication Failed"
        /// </remarks>
        /// <param name="jCredentials">JSON username password</param>
        /// <returns>
        /// </returns>
        public async Task<ActionResult> Login([FromBody]JObject jCredentials)
        {
            try
            {
                _logger.LogInformation("Login() called from: " + HttpContext.Connection.RemoteIpAddress.ToString());

                var received = new { Username = string.Empty, Password = string.Empty };

                received = JsonConvert.DeserializeAnonymousType(jCredentials.ToString(Formatting.None), received);

                _logger.LogInformation($"Paramerters: {received.Username}");

                var disco = await DiscoveryClient.GetAsync(Configuration["TokenAuthService"]);

                if (disco.IsError)
                {
                    _logger.LogError($"Failed to detect identity server");

                    return new JsonResult(new
                    {
                        respcode = ResponseCodes.SystemError,
                        description = ResponseCodes.SystemError.DisplayName(),
                        token = string.Empty,
                        roles = string.Empty
                    });
                }

                var tokenClient = new TokenClient(disco.TokenEndpoint, Configuration["APIClientId"], Configuration["ClientSecret"]);

                var tokenResponse = await tokenClient.RequestResourceOwnerPasswordAsync(received.Username, received.Password);

                if (tokenResponse.IsError)
                {
                    _logger.LogError($"Error in getting auth token: {tokenResponse.Error}: {tokenResponse.ErrorDescription}, {tokenResponse.Exception?.Message}");
                    return new JsonResult(new
                    {
                        respcode = ResponseCodes.AuthenticationFailed,
                        description = ResponseCodes.AuthenticationFailed.DisplayName(),
                        token = string.Empty,
                        roles = string.Empty
                    });
                }

                _logger.LogInformation($"Returning token: {tokenResponse.AccessToken}");
                return new JsonResult(new
                {
                    respcode = ResponseCodes.Successful,
                    description = ResponseCodes.Successful.DisplayName(),
                    token = tokenResponse.AccessToken
                });
            }
            catch (Exception e)
            {
                _logger.LogError($"Generic exception handler invoked. {e.Message}: {e.StackTrace}");

                return new JsonResult(new
                {
                    respcode = ResponseCodes.SystemError,
                    description = ResponseCodes.SystemError.DisplayName(),
                    Error = e.Message
                });
            }
        }

    }

    [Route("c19server/logout")]
    [ApiController]
    [Authorize(AuthenticationSchemes = "Bearer")]
    public class LogoutController : ControllerBase
    {
        private IConfiguration Configuration;
        private readonly ILogger<LogoutController> _logger;
        private ApplicationDbContext Context { get; set; }

        private enum ResponseCodes
        {
            [Display(Name = "Successful")]
            Successful = 1200,
            [Display(Name = "System Error")]
            SystemError = 1201,
            [Display(Name = "Invalid Token")]
            InvalidToken = 1202,
        }

        public LogoutController(IConfiguration configuration, ApplicationDbContext applicationDbContext, ILogger<LogoutController> logger)
        {
            Configuration = configuration;
            Context = applicationDbContext;
            _logger = logger;
        }

        /// <summary>
        /// Logout API. Logout the user. The auth token needs to be passed in.
        /// </summary>
        /// <remarks>
        /// Sample request:
        /// 
        ///     POST c19server/logout
        ///      
        /// Sample response:
        /// 
        ///     {
        ///        "respcode" = "1200",
        ///        "description" = "successful"
        ///     }
        ///     
        /// Response codes:
        ///     1200: Successful
        ///     1205: System Error
        ///     1206: Invalid token
        /// </remarks>
        /// <param name="jUserName">JSON user name.</param>
        /// <returns>
        /// </returns>
        /// 
        public async Task<ActionResult<string>> Logout()
        {
            try
            {
                _logger.LogInformation("Logout() called from: " + HttpContext.Connection.RemoteIpAddress.ToString());

                var authString = HttpContext.Request.Headers["Authorization"][0];
                var authToken = authString.Split(' ')[1].Trim();

                var client = new HttpClient();

                var instrospectionResult = await client.IntrospectTokenAsync(new TokenIntrospectionRequest
                {
                    Address = Configuration["TokenAuthService"] + "/" + Configuration["IntrospectionRelativeUrl"],
                    ClientId = Configuration["APIName"],
                    ClientSecret = Configuration["ClientSecret"],
                    Token = authToken,
                });

                if (!instrospectionResult.IsActive)
                {
                    _logger.LogError($"Session with token {authToken} is not active");
                    return new JsonResult(new
                    {
                        respcode = ResponseCodes.InvalidToken,
                        description = ResponseCodes.InvalidToken.DisplayName()
                    });
                }

                var result = await client.RevokeTokenAsync(new TokenRevocationRequest
                {
                    Address = Configuration["TokenAuthService"] + Configuration["RevocationRelativeUrl"],
                    ClientId = Configuration["APIClientId"],
                    ClientSecret = Configuration["ClientSecret"],

                    Token = authToken
                });

                if (result.IsError)
                {
                    _logger.LogError($"Token revocation failed. {result.Error}: {result.Exception?.ToString()}");
                    return new JsonResult(new
                    {
                        respcode = ResponseCodes.SystemError,
                        description = ResponseCodes.SystemError.DisplayName()
                    });
                }

                return new JsonResult(new
                {
                    respcode = ResponseCodes.Successful,
                    description = ResponseCodes.Successful.DisplayName()
                });
            }
            catch (Exception e)
            {
                _logger.LogError($"Generic exception handler invoked. {e.Message}: {e.StackTrace}");

                return new JsonResult(new
                {
                    respcode = ResponseCodes.SystemError,
                    description = ResponseCodes.SystemError.DisplayName(),
                    Error = e.Message
                });
            }

        }
    }
}