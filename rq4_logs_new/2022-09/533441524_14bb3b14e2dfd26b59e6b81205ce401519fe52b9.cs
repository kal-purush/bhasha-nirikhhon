using AutoMapper;
using Common.FluentValidation;
using DTO.Contract;
using MediatR;
using Microsoft.Extensions.Logging;
using System;
using System.Security.Principal;
using System.Threading.Tasks;
using System.Threading;
using DomainContract.Contexts;
using FluentValidation;
using System.Linq;
using DomainContract.Entities;
using APIContract.Utils;

namespace APIContract.Commands
{
    public class CreateUserCommand : IRequest<UserDto>
    {
        public Guid Id { get; set; }

        public string UserName { get; set; }

        public string Password { get; set; }

        public string Email { get; set; }
    }

    //fluent validation
    public class CreateUserCommandValidator : AbstractModelValidator<CreateUserCommand>
    {
        private readonly ContractDbContext _contractDbContext;
        public CreateUserCommandValidator(ContractDbContext contractDbContext)
        {
            _contractDbContext = contractDbContext;

            RuleFor(x => x.Email)
                .NotEmpty()
                .Must(val =>
                {
                    User user = _contractDbContext.Users.FirstOrDefault(x => x.Email == val);
                    return user == null;
                });

            RuleFor(x => x.UserName)
                .NotEmpty()
                .Must(val =>
                {
                    User user = _contractDbContext.Users.FirstOrDefault(x => x.UserName == val);
                    return user == null;
                });
        }
    }

    //command handler
    public class CreateUserCommandHandler : IRequestHandler<CreateUserCommand, UserDto>
    {
        private readonly ContractDbContext _contractDbContext;
        private readonly ILogger<CreateUserCommandHandler> _logger;
        private readonly IMapper _mapper;
        public CreateUserCommandHandler(ContractDbContext contractDbContext, ILogger<CreateUserCommandHandler> logger, IMapper mapper)
        {
            _contractDbContext = contractDbContext;
            _logger = logger;
            _mapper = mapper;
        }

        public async Task<UserDto> Handle(CreateUserCommand request, CancellationToken cancellationToken)
        {
           await UserUtil.IsUserExist(_contractDbContext, request.UserName);

            User user = new User()
            {
                UserName = request.UserName,
                Email = request.Email,
                Password = request.Password

            };
            _contractDbContext.Users.Add(user);

            //commit transaction
            await _contractDbContext.SaveChangesAsync();

            return _mapper.Map<UserDto>(user);
        }
    }
}