using AuthService.Models;
using AuthService.Requests;
using AuthService.Responses;
using Microsoft.AspNetCore.Identity.Data;

namespace AuthService.Abstracts;

public interface IAuthService
{
    Task<AuthServerReponseModel<TokenResponseModel>?> GetAccesToken(string username, string password);
    Task<bool> ValidateToken(string token);
    Task<AuthServerReponseModel<StatusModel>?> Register(RegisterRequest request, CancellationToken cancellationToken = default);
    Task<AuthServerReponseModel<StatusModel>?> Logout(LogoutRequest logoutRequest);
    Task<AuthServerReponseModel<StatusModel>?> UpdateUser(UpdateUserModel model);
    Task<AuthServerReponseModel<StatusModel>?> ResetPassword(ResetPasswordModel model);
    Task<AuthServerReponseModel<StatusModel>?> ResetPasswordWithEmail();
    Task<GetAccessTokenResponseModel> GetMainToken(CancellationToken cancellationToken = default);
    Task<AuthServerReponseModel<GroupRepresentationModel>?> AddGroup(GroupRepresentationModel model, CancellationToken cancellationToken = default);
    Task<AuthServerReponseModel<List<GroupRepresentationModel>>?> GetGroups(GetGroupListByFilterRequest model, CancellationToken cancellationToken = default);
    Task<AuthServerReponseModel<StatusModel>?> AddUserToGroup(AddUserToGroupRequestModel model, CancellationToken cancellationToken = default);
}