using IdentityManagementAPI.ModelResources;
using IdentityManagementAPI.Wrappers;

namespace IdentityManagementAPI.Services;

public interface IKeycloackService
{
    Task<string> GetAccessToken(CancellationToken cancellationToken = default);
    Task<Response<List<UserModel>>> GetUsers(CancellationToken cancellationToken = default);
    Task<Response<GetAccessTokenResponseModel>> Login(LoginModel loginModel, CancellationToken cancellationToken = default);
    Task<Response<StatusModel>> Register(RegisterModel registerModel, CancellationToken cancellationToken = default);
    Task<Response<List<UserModel>>> GetUserByEmail(string email, CancellationToken cancellationToken = default);
    Task<Response<List<UserModel>>> GetUserByUsername(string username, CancellationToken cancellationToken = default);
    Task<Response<UserModel>> GetUserById(Guid userId, CancellationToken cancellationToken = default);
    Task<Response<StatusModel>> UpdateUser(UpdateUserModel registerModel, CancellationToken cancellationToken = default);
    Task<Response<StatusModel>> DeleteUserById(Guid userId, CancellationToken cancellationToken = default);
    Task<Response<List<RoleModel>>> GetRoles(CancellationToken cancellationToken = default);
    Task<Response<List<RoleModel>>> GetRoleByName(string name, CancellationToken cancellationToken = default);
    Task<Response<StatusModel>> CreateRole(CreateRoleModel roleModel, CancellationToken cancellationToken = default);
    Task<Response<StatusModel>> DeleteRoleByName(string name, CancellationToken cancellationToken = default);
    Task<Response<StatusModel>> AssignmentRolesByUserId(UserRoleModel userRoleModel, CancellationToken cancellationToken = default);
    Task<Response<StatusModel>> UnAssignmentRolesByUserId(UnAssignmentRolesByUserId userRoleModel, CancellationToken cancellationToken = default);
    Task<Response<List<UserRoleDetailModel>>> GetAllUserRolesByUserId(Guid id, CancellationToken cancellationToken = default);
    Task<Response<StatusModel>> Logout(LogoutModel model, CancellationToken cancellationToken = default);
    Task<Response<StatusModel>> SendResetPasswordEmail(CancellationToken cancellationToken = default);
    Task<Response<StatusModel>> ResetPassword(ResetPasswordModel model, CancellationToken cancellationToken = default);
    Task<Response<GroupRepresentationModel>> AddGroup(GroupRepresentationModel model, CancellationToken cancellationToken = default);
    Task<Response<List<GroupRepresentationModel>>> GetGroups(GetGroupListByFilterRequest getGroupListByFilterRequest, CancellationToken cancellationToken = default);
    Task<Response<List<GroupRepresentationModel>>> GetGroupChildren(string groupId, GetGroupChildrenListByFilterRequest getGroupChildrenListByFilterRequest, CancellationToken cancellationToken = default);
    Task<Response<GroupRepresentationModel>> AddGroupChildren(string groupId, GroupRepresentationModel model, CancellationToken cancellationToken = default);
    Task<Response<GroupRepresentationModel>> GetGroup(string groupId, CancellationToken cancellationToken = default);
    Task<Response<ManagementPermissionReferenceModel>> GetGroupManagementPermission(string groupId, CancellationToken cancellationToken = default);
    Task<Response<ManagementPermissionReferenceModel>> UpdateGroupManagementPermission(string groupId, ManagementPermissionReferenceModel model, CancellationToken cancellationToken = default);
    Task<Response<List<UserRepresentationModel>>> GetGroupUsers(string groupId, GetGroupMembersByFilter getGroupMembersByFilter, CancellationToken cancellationToken = default);
    Task<Response<GroupRepresentationModel>> UpdateGroup(string groupId, GroupRepresentationModel model, CancellationToken cancellationToken = default);
    Task<Response<MappingsRepresentationModel>> GetGroupRoleMapping(string groupId, CancellationToken cancellationToken = default);
    Task<Response<List<RoleRepresentationModel>>> GetGroupRoleMappingRealmAvailable(string groupId, CancellationToken cancellationToken = default);
    Task<Response<List<RoleRepresentationModel>>> GetGroupRoleMappingRealmComposite(string groupId, CancellationToken cancellationToken = default);
    Task<Response<List<RoleRepresentationModel>>> GetGroupRoleMappingRealm(string groupId, CancellationToken cancellationToken = default);
    Task<Response<StatusModel>> AddGroupRoleToMappingRealm(string groupId, RoleRepresentationModel model, CancellationToken cancellationToken = default);
    Task<Response<List<MappingsRepresentationModel>>> GetUserRoleMappings(string userId, CancellationToken cancellationToken = default);
    Task<Response<List<RoleRepresentationModel>>> GetUserRoleMappingsRealmAvaible(string userId, CancellationToken cancellationToken = default);
    Task<Response<StatusModel>> AddUserToGroup(string userId, string groupId, CancellationToken cancellationToken = default);
}