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
    public class AdminGuideBookingService: IAdminGuideBookingService
    {
        private readonly IApplicationUnitOfWork _applicationUnitOfWork;
        public AdminGuideBookingService(IApplicationUnitOfWork applicationUnitOfWork)
        {
            _applicationUnitOfWork = applicationUnitOfWork;
        }

        public GuideBooking Get(Guid id)
        {
            return _applicationUnitOfWork.GuideBookingRepository.GetById(id);
        }

        public IEnumerable<GuideBooking> GetAll()
        {
            return _applicationUnitOfWork.GuideBookingRepository.GetAll();
        }
    }
}