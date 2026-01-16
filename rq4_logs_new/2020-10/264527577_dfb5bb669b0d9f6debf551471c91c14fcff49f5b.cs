namespace FS.Api.Commands.Handlers
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Command;
    using Domain.Core.Interfaces;
    using Domain.Model;
    using Domain.Model.Validators;
    using FluentValidation;
    using MediatR;

    public class CreateAccountCommandHandler : IRequestHandler<CreateAccountCommand, Guid>
    {
        private readonly IAccountRepository _accountRepository;
        private readonly IUserRepository _userRepository;

        public CreateAccountCommandHandler(IAccountRepository accountRepository, IUserRepository userRepository)
        {
            _accountRepository = accountRepository;
            _userRepository = userRepository;
        }

        public async Task<Guid> Handle(CreateAccountCommand command, CancellationToken cancellationToken)
        {
            var user = await _userRepository.Get(command.UserId);
            var account = new Account(user);

            var validator = new AccountValidator();
            var result = await validator.ValidateAsync(account, default);

            if (!result.IsValid)
            {
                throw new Exception("Model invalid.");
            }
            
            await _accountRepository.Insert(account);
            return account.Id;
        }
    }
}