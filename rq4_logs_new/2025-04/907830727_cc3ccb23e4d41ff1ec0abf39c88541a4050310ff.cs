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
    public class BookingService : IGetService<Booking, Guid>
    {
        private readonly IApplicationUnitOfWork _applicationUnitOfWork;
        public BookingService(IApplicationUnitOfWork applicationUnitOfWork)
        {
            _applicationUnitOfWork = applicationUnitOfWork;
        }
        public Booking Get(Guid id)
        {
            return _applicationUnitOfWork.BookingRepository.GetById(id);
        }

        public IEnumerable<Booking> GetAll()
        {
            return _applicationUnitOfWork.BookingRepository.GetAll();
        }
    }
}