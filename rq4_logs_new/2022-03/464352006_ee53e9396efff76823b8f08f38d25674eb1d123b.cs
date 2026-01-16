using AutoMapper;
using GetFood.Business.Abstract;
using GetFood.Entities.Base;
using GetFood.Entities.Dtos;
using GetFood.Entities.IBase;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;

namespace GetFood.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
    public class OrderController : ControllerBase
    {
        private readonly IOrderService service;
        private readonly IUserService userService;
        private readonly IMapper mapper;
        public OrderController(IOrderService service, IUserService userService, IMapper mapper)
        {
            this.service = service;
            this.userService = userService;
            this.mapper = mapper;
        }


        [HttpPost("{foodId}")]
        public IResponse<OrderDto> OrderFood(int foodId)
        {
            ClaimsIdentity identity = HttpContext.User.Identity as ClaimsIdentity;
            int userId = userService.ReadUserToken(identity);
            var response = service.OrderFood(userId, foodId);
            return response;

        }


        [HttpGet]
        public IResponse<List<OrderDto>> GetOrderHistory()
        {
            ClaimsIdentity identity = HttpContext.User.Identity as ClaimsIdentity;
            int userId = userService.ReadUserToken(identity);

            var orderList = service.GetOrderHistory(userId);
            var orderListDto = orderList.Select(x => mapper.Map<OrderDto>(x)).ToList();

            var response = new Response<List<OrderDto>>
            {
                Message = "Success",
                StatusCode = StatusCodes.Status200OK,
                Data = orderListDto
            };

            return response;

        }




    }
}