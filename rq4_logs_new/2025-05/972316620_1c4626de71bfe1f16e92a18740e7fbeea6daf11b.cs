using IdentityManagementAPI.ModelResources;
using IdentityManagementAPI.Services.Abstracts;
using IdentityManagementAPI.Wrappers;

namespace IdentityManagementAPI.Endpoints;

public static class RoleEndpointMaps
{
    public static IEndpointRouteBuilder RoleEndpointBuilders(this IEndpointRouteBuilder builder)
    {
        builder.MapGet("/api/role",
            async (IKeycloackService service, CancellationToken cancellationToken) =>
            {
                var result = await service.GetRoles(cancellationToken);
                return result.ToIResult();
            });

        builder.MapGet("/api/role/{name}",
            async (IKeycloackService service, string name, CancellationToken cancellationToken) =>
            {
                var result = await service.GetRoleByName(name, cancellationToken);
                return result.ToIResult();
            });

        builder.MapPost("/api/role", async (IKeycloackService service, CreateRoleModel request,
            CancellationToken cancellationToken) =>
        {
                var result = await service.CreateRole(request, cancellationToken);
                return result.ToIResult();
        });

        builder.MapDelete("/api/role/{name}",
            async (IKeycloackService service, string name, CancellationToken cancellationToken) =>
            {
                var result = await service.DeleteRoleByName(name, cancellationToken);
                return result.ToIResult();
            });

        builder.MapGet("/api/role/group-role-mapping/{groupId}",
            async (IKeycloackService service, string groupId, CancellationToken cancellationToken) =>
            {
                var result = await service.GetGroupRoleMapping(groupId, cancellationToken);
                return result.ToIResult();
            });
        
        builder.MapGet("/api/role/group-role-mapping-realm-available/{groupId}",
            async (IKeycloackService service, string groupId, CancellationToken cancellationToken) =>
            {
                var result = await service.GetGroupRoleMappingRealmAvailable(groupId, cancellationToken);
                return result.ToIResult();
            });
        
        builder.MapGet("/api/role/group-role-mapping-realm-composite/{groupId}",
            async (IKeycloackService service, string groupId, CancellationToken cancellationToken) =>
            {
                var result = await service.GetGroupRoleMappingRealmComposite(groupId, cancellationToken);
                return result.ToIResult();
            });
        
        builder.MapGet("/api/role/group-role-mapping-realm/{groupId}",
            async (IKeycloackService service, string groupId, CancellationToken cancellationToken) =>
            {
                var result = await service.GetGroupRoleMappingRealm(groupId, cancellationToken);
                return result.ToIResult();
            });
        
        builder.MapPost("/api/role/add-group-role-mapping-realm", async (IKeycloackService service, RoleRepresentationModel request,
            CancellationToken cancellationToken) =>
        {
            var result = await service.AddGroupRoleToMappingRealm(request.GroupId!, request, cancellationToken);
            return result.ToIResult();
        });
        
        return builder;
    }
}