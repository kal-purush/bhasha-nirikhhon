using MediatR;
using Sample.Domain.Interfaces;
using Sample.Domain.Models;

namespace Sample.Application.Commands.Handlers
{
    public class EditUserCommandHandler(IRepository<User> repository) : IRequestHandler<EditUserCommand, Response<User>>
    {
        private readonly IRepository<User> _repository = repository;
        public async Task<Response<User>> Handle(EditUserCommand request, CancellationToken cancellationToken)
        {
            User targetUser = await _repository.FindByIdAsync(request.Id) ?? new();

            UserMapper(request, targetUser);

            await _repository.UpdateAsync(targetUser);

            return new() { Data = targetUser, Message = "", Success = true };
        }

        private static User UserMapper(EditUserCommand request, User targetUser)
        {
            targetUser.Name = request.Name;
            targetUser.Email = request.Email;
            targetUser.Number = request.Number;

            return targetUser;
        }
    }
}