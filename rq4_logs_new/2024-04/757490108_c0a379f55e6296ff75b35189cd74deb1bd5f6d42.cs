using BuildWolf.Modules.UserModules;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace BuildWolf.BAT.Interfaces
{
    public interface ISearchedUserPage
    {
        Task<bool> CreateArchitectWorkedMapping(UserArchitectWorkedMapping data);
        Task<IEnumerable<UserArchitectWorkedMapping>> GetAllArchitectWorkedMapping();
        Task<bool> DeleteArchitectWorkedMapping(Guid id);

        Task<bool> CreateUserProjectWork(UserProjectMapping data);
        Task<IEnumerable<UserProjectMapping>> GetAllUserProjectWork();
        Task<UserProjectMapping> UpdateUserProject(UserProjectMapping data);
        Task<bool> DeleteUserProject(Guid id);

        Task<bool> CreateUserReview(UserReviewMapping data);
        Task<IEnumerable<UserReviewMapping>> GetAllUserReview();
        Task<UserReviewMapping> UpdateUserReview(UserReviewMapping data);
        Task<bool> DeleteUserReview(Guid id);

        Task<bool> CreateUserServiceMapping(UserServiceMapping data);
        Task<IEnumerable<UserServiceMapping>> GetAllService();
        Task<UserServiceMapping> UpdateUserService(UserServiceMapping data);
        Task<bool> DeleteUserService(Guid id);

        Task<SearchedUserPageDisplay> GetUserPageDisplayData(SearchedUserPageDisplay data);
    }
}