using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using home_swap_api.Dto;
using home_swap_api.interfaces;
using home_swap_api.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace home_swap_api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class OfferController : ControllerBase
    {
        private readonly IUnitOfWork uow;
        private readonly IMapper mapper;
        public OfferController(IUnitOfWork uow, IMapper mapper)
        {
            this.uow = uow;
            this.mapper = mapper;
        }

        [HttpGet]
        public async Task<IActionResult> GetOffers()
        {
            var offers = await uow.OfferRepository.GetOffersAsync();
            var offersDTO = mapper.Map<IEnumerable<OfferDTO>>(offers);

            //throw new Exception("Some unknow error");

            return Ok(offersDTO);
        }

        [HttpPost]
        public async Task<IActionResult> AddOffer([FromBody] OfferDTO offerDTO)
        {
            //throw new UnauthorizedAccessException();
            var offer = mapper.Map<Offer>(offerDTO);


            uow.OfferRepository.AddOffer(offer);
            await uow.SaveAsync();

            return StatusCode(201);
        }

        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> DeleteOffer(int id)
        {
            uow.OfferRepository.DeleteOffer(id);
            await uow.SaveAsync();

            return Ok(id);
        }
    }
}