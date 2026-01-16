using MediatR;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using ProjectCommons.CommonServices;
using ProjectCommons.CommonModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Project1.Controllers
{
    public class Hotel : Controller
    {
        readonly IMediator _mediatr;
        public Hotel(IMediator mediator)
        {
            _mediatr = mediator;
        }

        /// <summary>
        /// Posting Hotel Details
        /// </summary>
        /// <returns></returns>
        [HttpPost("/hotel")]
        [Consumes("application/json")]
        [AllowAnonymous]
        public async Task<PostHotel.Response> HotelRegister([FromBody] HotelDetailsRequest hotelDetails)
        {

            var service = new PostHotel.Request(hotelDetails);
            var response = await _mediatr.Send(service);
            return response;

        }

        /// <summary>
        /// Posting Customer Details
        /// </summary>
        /// <returns></returns>
        [HttpPost("/booking")]
        [Consumes("application/json")]
        [AllowAnonymous]
        public async Task<PostCustome.Response> CustomerRegister([FromBody] CustomerDetailsRequest hotelDetails)
        {

            var service = new PostCustome.Request(hotelDetails);
            var response = await _mediatr.Send(service);
            return response;

        }
        /// <summary>
        /// Get Hotel Details
        /// </summary>
        /// <returns></returns>
        [HttpGet("/hoteldetails")]
        [AllowAnonymous]
        public async Task<GetHotel.Response> HotelDetail()
        {
            var service = new GetHotel.Request();
            var response = await _mediatr.Send(service);
            return response;
        }

        /// <summary>
        /// Get Individual Hotel
        /// </summary>
        /// <returns></returns>
        /// <param name="hotelName">Type of Getting Record</param>
        /// /// <param name="hotelId">Type of Getting Record</param>
        [HttpGet("/hotel")]
        [AllowAnonymous]
        public async Task<GetHotelSearch.Response> HotelSearch([FromQuery] string hotelName, [FromQuery] Guid hotelId)
        {
            var service = new GetHotelSearch.Request(hotelName, hotelId);
            var response = await _mediatr.Send(service);
            return response;
        }


        /// <summary>
        ///Delete Hotel
        /// </summary>
        /// <returns></returns>
        /// <param name="hotelName">Type of Getting Record</param>
        /// /// <param name="hotelId">Type of Getting Record</param>
        [HttpDelete("/hotel")]
        public async Task<DeleteHotel.Response> DeleteSettings([FromQuery] string hotelName, [FromQuery] Guid hotelId)
        {

            var service = new DeleteHotel.Request(hotelName, hotelId);
            return await _mediatr.Send(service);

        }
        /// <summary>
        /// Posting Hotel Details
        /// </summary>
        /// <returns></returns>
        [HttpPut("/hotel")]
        [Consumes("application/json")]
        [AllowAnonymous]
        public async Task<PutHotel.Response> HotelUpdate([FromBody] HotelDetailsRequestPut hotelDetails)
        {

            var service = new PutHotel.Request(hotelDetails);
            var response = await _mediatr.Send(service);
            return response;

        }
    }
}