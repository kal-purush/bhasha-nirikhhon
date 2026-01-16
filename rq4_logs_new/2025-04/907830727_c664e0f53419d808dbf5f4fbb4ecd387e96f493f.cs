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
    public class AdminBusBookingService: IAdminBusBookingService
    {
        private readonly IApplicationUnitOfWork _applicationUnitOfWork;
        public AdminBusBookingService(IApplicationUnitOfWork applicationUnitOfWork)
        {
            _applicationUnitOfWork = applicationUnitOfWork;
        }

        public BusBooking Get(Guid id)
        {
            return _applicationUnitOfWork.BusBookingRepository.GetById(id);
        }

        public IEnumerable<BusBooking> GetAll()
        {
            return _applicationUnitOfWork.BusBookingRepository.GetAll();
        }
    }
}