using Domain;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.Commands.UpdateUserCommand
{
    public class UpdateUserDisplayNameCommandHandler : IRequestHandler<UpdateUserDisplayNameCommand, User>
    {
        private readonly IUnitOfWork _unitOfWork;

        public UpdateUserDisplayNameCommandHandler(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        public async Task<User> Handle(UpdateUserDisplayNameCommand info, CancellationToken cancellationToken)
        {
            var user = await _unitOfWork.UserRepository.UpdateDisplayName(info.IdUser, info.NewDisplayName);

            if (user == null)
            {
                return null;
            }

            return user;
        }
    }
}