using MediatR;
using Photos.Domain.DataBaseEntity;
using Photos.Domain.Repositories;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Photos.Application.Queries.Photos
{
    public class GetPhotosQueryHandler : IRequestHandler<GetPhotosQuery, IEnumerable<PhotoEntity>>
    {
        private readonly IUnitOfWork _unitOfWork;

        public GetPhotosQueryHandler(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }
        public async Task<IEnumerable<PhotoEntity>> Handle(GetPhotosQuery request, CancellationToken cancellationToken)
        {
            return await _unitOfWork.PhotoRepository.GetPhotosAsync(cancellationToken);
        }
    }
}