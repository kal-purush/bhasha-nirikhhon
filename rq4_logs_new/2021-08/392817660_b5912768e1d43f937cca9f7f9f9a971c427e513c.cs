using AutoMapper;
using BestUzdNew.Api.Dtos.Input;
using BestUzdNew.Domain.Entities;
using BestUzdNew.Logic;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BestUzdNew.Api.Controllers
{
    [Route("api/serviceDiscount")]
    [ApiController]
    public class ServiceDiscountController : ControllerBase
    {
        private readonly IMapper _mapper;
        private readonly IDiscountService _discounttService;

        public ServiceDiscountController(IMapper mapper, IDiscountService discounttService)
        {
            _mapper = mapper;
            _discounttService = discounttService;
        }

        [HttpPost]
        public async Task<IActionResult> CreateDiscount(ServiceDiscountInDto discountInDto)
        {
            var discount = _mapper.Map<ServiceDiscountInDto, ServiceDiscount>(discountInDto);
            await _discounttService.CreateDiscountServiceAsync(discount);

            return Ok();
        }

        [HttpGet]
        public async Task<IActionResult> GetOrders()
        {
            var discounts = await _discounttService.GetDiscountServiceAsync();
            var discountsDto = _mapper.Map<IEnumerable<ServiceDiscount>, IEnumerable<ServiceDiscountInDto>>(discounts);

            return Ok(discountsDto);
        }
    }
}