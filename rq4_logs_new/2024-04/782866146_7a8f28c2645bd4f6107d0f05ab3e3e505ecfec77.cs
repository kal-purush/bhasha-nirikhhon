using ConferenceRoomBooking.Repositories;
using Microsoft.AspNetCore.Mvc;
using ConferenceRoomBooking.Models;


namespace ConferenceRoomBooking.Controllers
{
    public class BookingsController
    {
        [ApiController]
        [Route("api/Bookings")]
        public class BookingController : ControllerBase
        {
            private readonly BookingsRepository _bookingsRepository;
            public BookingController(BookingsRepository bookingsRepository)
            {
                _bookingsRepository = bookingsRepository;
            }
        }
    }
 
}