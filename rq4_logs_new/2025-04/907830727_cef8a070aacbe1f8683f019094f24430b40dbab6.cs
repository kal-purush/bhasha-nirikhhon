using EasyTravel.Domain;
using EasyTravel.Domain.Entites;
using EasyTravel.Domain.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EasyTravel.Application.Services
{
    public class AdminPhotographerBookingService: IAdminPhotographerBookingService
    {
        private readonly IApplicationUnitOfWork _applicationUnitOfWork;
        public AdminPhotographerBookingService(IApplicationUnitOfWork applicationUnitOfWork)
        {
            _applicationUnitOfWork = applicationUnitOfWork;
        }

        public PhotographerBooking Get(Guid id)
        {
            return _applicationUnitOfWork.PhotographerBookingRepository.GetById(id);
        }

        public IEnumerable<PhotographerBooking> GetAll()
        {
            return _applicationUnitOfWork.PhotographerBookingRepository.GetAll();
        }
    }
}