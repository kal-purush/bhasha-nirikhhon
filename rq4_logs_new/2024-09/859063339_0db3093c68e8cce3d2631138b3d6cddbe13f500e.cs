using TripEnjoy.Application.Data;
using TripEnjoy.Application.Interface;

namespace TripEnjoy.Application.Services
{
    public class AccountService : IAccountService
    {
        private readonly IAccountRepository accountRepository;
       

        public AccountService(IAccountRepository accountRepository )
        {
            this.accountRepository = accountRepository;
        }
       

        public Task<bool> Register(AccountDTO account)
        {
            
            throw new NotImplementedException();
        }

        Task<string> IAccountService.Login(AccountDTO account)
        {
         
            return accountRepository.Login(account);
        }
    }
}