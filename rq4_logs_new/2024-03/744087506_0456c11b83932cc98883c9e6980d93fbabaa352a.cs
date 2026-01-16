using TaskManager.Domain.Entities;
using TaskManager.Domain.Interfaces;

namespace TaskManager.Domain.Services
{
    public class AccountService
    {
        private readonly IAccountRepository _accountRepository;

        public AccountService(IAccountRepository accountRepository)
        {
            _accountRepository = accountRepository
                ?? throw new ArgumentNullException(nameof(accountRepository));
        }

        public async Task<Account> GetAccountById(Guid id, CancellationToken cancellationToken)
        {
            var existedAccount = _accountRepository.FindAccountById(id, cancellationToken);
            if (existedAccount is null)
            {
                throw new ArgumentNullException(nameof(existedAccount));
            }
            return await _accountRepository.GetById(id, cancellationToken);
        }

        public async Task<Account> GetAccountByEmail(string email, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(email))
            {
                throw new ArgumentNullException(nameof(email));
            }

            var existedAccount = _accountRepository.FindAccountByEmail(email, cancellationToken);
            if (existedAccount is null)
            {
                throw new ArgumentNullException(nameof(existedAccount));
            }
            return await _accountRepository.GetAccountByEmail(email, cancellationToken);
        }
    }
}