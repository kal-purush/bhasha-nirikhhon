using MediatR;
using Photos.Domain.DataBaseEntity;
using Photos.Domain.Repositories;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Photos.Application.Commands.Photos
{
    public class AddPhotoCommandHandler : IRequestHandler<AddPhotoCommand, int>
    {
        private readonly IUnitOfWork _unitOfWork;

        public AddPhotoCommandHandler(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        public async Task<int> Handle(AddPhotoCommand request, CancellationToken cancellationToken)
        {
            var photos = new PhotoEntity
            {
                Name = request.Name,
                CreateDate = request.CreateDate,
                URL = request.URL
            };

            await _unitOfWork.PhotoRepository.AddPhotoAsync(photos,cancellationToken);

            //TODO сменить на более адекватный
            return photos.Id;
        }
    }
}