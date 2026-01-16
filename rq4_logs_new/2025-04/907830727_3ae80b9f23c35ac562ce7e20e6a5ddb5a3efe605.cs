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
    public class AdminCarBookingService: IAdminCarBookingService
    {
        private readonly IApplicationUnitOfWork _applicationUnitOfWork;
        public AdminCarBookingService(IApplicationUnitOfWork applicationUnitOfWork)
        {
            _applicationUnitOfWork = applicationUnitOfWork;
        }

        public CarBooking Get(Guid id)
        {
            return _applicationUnitOfWork.CarBookingRepository.GetById(id);
        }

        public IEnumerable<CarBooking> GetAll()
        {
            return _applicationUnitOfWork.CarBookingRepository.GetAll();
        }
    }
}