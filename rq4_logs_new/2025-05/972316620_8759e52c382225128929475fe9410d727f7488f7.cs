using IdentityManagementAPI.ModelResources;
using IdentityManagementAPI.Services.Abstracts;
using IdentityManagementAPI.Wrappers;

namespace IdentityManagementAPI.Endpoints;

public static class UserRoleEndpointMaps
{
    public static IEndpointRouteBuilder UserRoleEndpointBuilder(this IEndpointRouteBuilder builder)
    {
        builder.MapPost("/api/user-role/assigment-roles-by-userid", async (IKeycloackService service, UserRoleModel request,
            CancellationToken cancellationToken) =>
        {
            var result = await service.AssignmentRolesByUserId(request, cancellationToken);
            return result.ToIResult();
        }).RequireAuthorization();
        
        builder.MapPost("/api/user-role/un-assigment-roles-by-userid", async (IKeycloackService service, UnAssignmentRolesByUserId request,
            CancellationToken cancellationToken) =>
        {
            var result = await service.UnAssignmentRolesByUserId(request, cancellationToken);
            return result.ToIResult();
        }).RequireAuthorization();
        
        builder.MapGet("/api/user-role/get-all-user-roles-by-userid/{id}",
            async (IKeycloackService service, Guid id, CancellationToken cancellationToken) =>
            {
                var result = await service.GetAllUserRolesByUserId(id, cancellationToken);
                return result.ToIResult();
            }).RequireAuthorization();
        
        return builder;
    }
}