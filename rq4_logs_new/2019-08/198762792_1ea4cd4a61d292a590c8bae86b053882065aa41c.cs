using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TicketingSystem.ViewModel.ViewModel;
using TicketingSystem.ViewModel.ViewModels;

namespace TicketingSystem.BLL.Contracts
{
    public interface IUserService<TVM, LVM, TType> where TVM : class where LVM : class where TType : IConvertible
    {
        //IEnumerable<TVM> GetAll(UserVM userVM);

        TVM GetSingleBy(string id);

        Task<ResponseVM> Register(TVM entity);

        Task<ResponseVM> Delete(string id);
        Task<ResponseVM> Update(TVM entity);

        Task<ResponseVM> Deactivate(TVM entity);
        Task<ResponseVM> Login(LVM entity);
        Task<ResponseVM> Validate(TVM entity);

        Task<TVM> UserProfile(string id);

        Task<ForgotPasswordVM> GetSecurityQuestion(string id);
        Task<ResponseVM> ResetPassword(ForgotPasswordVM entity);
        IEnumerable<UserVM> GetAll();
        
    }
}