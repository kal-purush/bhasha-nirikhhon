using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using OnlineStore.Domain.Common.Responses;
using OnlineStore.Domain.Constants;
using OnlineStore.Service.DTOs.Order;
using OnlineStore.Service.Interfaces;

namespace OnlineStore.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class OrdersController : ControllerBase
    {
        private readonly IOrderService orderService;

        public OrdersController(IOrderService orderService)
        {
            this.orderService = orderService;
        }

        [HttpPost]
        public async Task<JsonResponse> PostOrderAsync(OrderForCreationDto useDto)
        {
            try
            {
                var result = await orderService.AddOrderAsync(useDto);
                return JsonResponse.DataResponse(result, ResponseMessages.SUCCESS_ADD_DATA);
            }
            catch (Exception exception)
            {
                return JsonResponse.ErrorResponse(exception.Message);
            }
        }

        [HttpGet]
        public async Task<JsonResponse> GetAllOrders()
        {
            try
            {
                var result = await orderService.RetrieveAllOrders();
                return JsonResponse.DataResponse(result);
            }
            catch (Exception exception)
            {
                return JsonResponse.ErrorResponse(exception.Message);
            }
        }

        [HttpGet("{orderId}")]
        public async Task<JsonResponse> GetOrderByIdAsync(int orderId)
        {
            try
            {
                var result = await orderService.RetrieveOrderByIdAsync(orderId);
                return JsonResponse.DataResponse(result);
            }
            catch (Exception exception)
            {
                return JsonResponse.ErrorResponse(exception.Message);
            }
        }

        [HttpPut("{orderId}")]
        public async Task<JsonResponse> PutOrderAsync(int orderId, OrderForUpdateDto orderDto)
        {
            try
            {
                var result = await orderService.ModifyOrderAsync(orderId, orderDto);
                return JsonResponse.DataResponse(result, ResponseMessages.SUCCESS_UPDATE_DATA);
            }
            catch (Exception exception)
            {
                return JsonResponse.ErrorResponse(exception.Message);
            }
        }

        [HttpDelete("{orderId}")]
        public async Task<JsonResponse> DeleteOrderByIdAsync(int orderId)
        {
            try
            {
                var result = await orderService.RemoveOrderByIdAsync(orderId);
                return JsonResponse.DataResponse(result, ResponseMessages.SUCCESS_DELETE_DATA);
            }
            catch (Exception ex)
            {
                return JsonResponse.ErrorResponse(ex.Message);
            }
        }
    }
}