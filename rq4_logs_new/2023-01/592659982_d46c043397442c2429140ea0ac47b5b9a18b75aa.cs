using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using OnlineStore.Domain.Common.Responses;
using OnlineStore.Domain.Constants;
using OnlineStore.Service.DTOs.Customer;
using OnlineStore.Service.Interfaces;

namespace OnlineStore.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CustomersController : ControllerBase
    {
        private readonly ICustomerService customerService;

        public CustomersController(ICustomerService customerService)
        {
            this.customerService = customerService;
        }

        [HttpPost]
        public async Task<JsonResponse> PostGuestAsync(CustomerForCreationDto useDto)
        {
            try
            {
                var result = await customerService.AddCustomerAsync(useDto);
                return JsonResponse.DataResponse(result, ResponseMessages.SUCCESS_ADD_DATA);
            }
            catch (Exception exception)
            {
                return JsonResponse.ErrorResponse(exception.Message);
            }
        }

        [HttpGet]
        public async Task<JsonResponse> GetAllCustomers()
        {
            try
            {
                var result = await customerService.RetrieveAllCustomers();
                return JsonResponse.DataResponse(result);
            }
            catch (Exception exception)
            {
                return JsonResponse.ErrorResponse(exception.Message);
            }
        }

        [HttpGet("id")]
        public async Task<JsonResponse> GetCustomerByIdAsync(int customerId)
        {
            try
            {
                var result = await customerService.RetrieveCustomerByIdAsync(customerId);
                return JsonResponse.DataResponse(result);
            }
            catch (Exception exception)
            {
                return JsonResponse.ErrorResponse(exception.Message);
            }
        }

        [HttpPut("id")]
        public async Task<JsonResponse> PutCustomerAsync(int customerId, CustomerForUpdateDTo customerDto)
        {
            try
            {
                var result = await customerService.ModifyCustomerAsync(customerId, customerDto);
                return JsonResponse.DataResponse(result, ResponseMessages.SUCCESS_UPDATE_DATA);
            }
            catch (Exception exception)
            {
                return JsonResponse.ErrorResponse(exception.Message);
            }
        }

        [HttpDelete("id")]
        public async Task<JsonResponse> DeleteCustomerByIdAsync(int id)
        {
            try
            {
                var result = await customerService.RemoveCustomerByIdAsync(id);
                return JsonResponse.DataResponse(result, ResponseMessages.SUCCESS_DELETE_DATA);
            }
            catch (Exception ex)
            {
                return JsonResponse.ErrorResponse(ex.Message);
            }
        }
    }
}