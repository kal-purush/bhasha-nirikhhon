using System;
using System.Threading.Tasks;
using Colabora.Infrastructure.Auth;
using Colabora.Infrastructure.Auth.Google;
using Colabora.Infrastructure.Auth.Shared;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Colabora.IntegrationTests.Fixtures;

public class AuthTokenFixture
{
    private readonly IConfigurationRoot _configuration;
    
    public AuthTokenFixture()
    {
        _configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.Test.json")
            .Build();
    }
    
    public async Task<string> GenerateTestJwt(string email,AuthProvider authProvider = AuthProvider.Google)
    {
        var googleService = Substitute.For<IGoogleAuthService>();
        var securityOptions = Substitute.For<IOptions<JwtSettings>>();
        securityOptions.Value.Returns(new JwtSettings
        {
            JwtAudience = _configuration.GetValue<string>("AuthenticationSettings:JwtAudience"),
            JwtIssuer = _configuration.GetValue<string>("AuthenticationSettings:JwtIssuer"),
            JwtKey = _configuration.GetValue<string>("AuthenticationSettings:JwtKey"),
            ExpirationTime = TimeSpan.FromMinutes(1)
        });
        
        googleService.Authenticate(Arg.Any<string>()).Returns(new UserAuthInfo(email, "test name"));
        
        var authService = new AuthService(googleService, securityOptions);
        var authResult = await authService.Authenticate(authProvider, "fake token");
        return authResult.Token;
    }
}