using System.Threading.Tasks;
using IdentityManagementAPI.ModelResources;
using IdentityManagementAPI.Services.Abstracts;
using IdentityManagementAPI.Wrappers;

namespace IdentityManagementAPI.Endpoints;

public static class AuthEnpointMaps
{
    public static IEndpointRouteBuilder AuthEndpoints(this IEndpointRouteBuilder builder){
        builder.MapPost("/api/auth/login",async (IKeycloackService service, LoginModel request, CancellationToken cancellationToken) =>{
            var result = await service.Login(request, cancellationToken);
            return result.ToIResult();
        });

        builder.MapPost("/api/auth/register", async (IKeycloackService service, RegisterModel request, CancellationToken cancellationToken)=>{
            var result = await service.Register(request, cancellationToken);
            return result.ToIResult();
        });

        builder.MapPost("/api/auth/logout", async(IKeycloackService service, LogoutModel request, CancellationToken cancellationToken) => {
            var result = await service.Logout(request, cancellationToken);
            return result.ToIResult();
        }).RequireAuthorization();

        builder.MapPost("/api/auth/sendresetpasswordemail", async(IKeycloackService service, CancellationToken cancellationToken)=>{
            var result = await service.SendResetPasswordEmail(cancellationToken);
            return result.ToIResult();
        }).RequireAuthorization();

         builder.MapPost("/api/auth/resetpasword", async(IKeycloackService service, ResetPasswordModel request, CancellationToken cancellationToken) => {
            var result = await service.ResetPassword(request, cancellationToken);
            return result.ToIResult();
        }).RequireAuthorization();

        return builder;
    }
}