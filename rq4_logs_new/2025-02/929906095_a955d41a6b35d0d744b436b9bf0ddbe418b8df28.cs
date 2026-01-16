using System;
using BusinessObject.Entities;
using BusinessObject.Interfaces;
using Microsoft.AspNetCore.Mvc;


namespace API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class BookingController(IRepository<Booking> _bookingRepository,
                                   IRepository<Voucher> _voucherRepository) : ControllerBase
    {
        [HttpPut("confirm-booking-status")]
        public async Task<IActionResult> ConfirmBookingStatus([FromQuery] Guid bookingID)
        {
            var getBooking = await _bookingRepository.GetByIdAsync(bookingID);
            if (getBooking != null && getBooking.Status.Equals("Booked"))
            {

                getBooking.Status = "Payment Completed";
                await _bookingRepository.UpdateAsync(getBooking);
                await _bookingRepository.SaveAsync();
                return Ok(new { Message = "Update Booking Status Success" });
            }

            return NotFound();
        }

        
        }
    }
