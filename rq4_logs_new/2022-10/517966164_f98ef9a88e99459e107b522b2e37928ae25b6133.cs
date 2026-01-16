using Azure;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.Commands.UpdateAvatarCommand
{
    public class UpdateAvatarCommandHandler : IRequestHandler<UpdateAvatarCommand, string>
    {
        private readonly IUnitOfWork _unitOfWork;

        public UpdateAvatarCommandHandler(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        public async Task<string> Handle(UpdateAvatarCommand info, CancellationToken cancellationToken)
        {
            var response = await _unitOfWork.UserRepository.UpdateAvatarAsync(info.IdUser, info.ImagePath);

            return response;
        }
    }
}