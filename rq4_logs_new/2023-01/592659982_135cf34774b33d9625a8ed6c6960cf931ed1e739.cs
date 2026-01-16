using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using OnlineStore.Domain.Common.Responses;
using OnlineStore.Domain.Constants;
using OnlineStore.Service.DTOs.Product;
using OnlineStore.Service.Interfaces;

namespace OnlineStore.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ProductsController : ControllerBase
    {
        private readonly IProductService productService;

        public ProductsController(IProductService productService)
        {
            this.productService = productService;
        }

        [HttpPost]
        public async Task<JsonResponse> PostProductAsync(ProductForCreationDto useDto)
        {
            try
            {
                var result = await productService.AddProductAsync(useDto);
                return JsonResponse.DataResponse(result, ResponseMessages.SUCCESS_ADD_DATA);
            }
            catch (Exception exception)
            {
                return JsonResponse.ErrorResponse(exception.Message);
            }
        }

        [HttpGet]
        public async Task<JsonResponse> GetAllProducts()
        {
            try
            {
                var result = await productService.RetrieveAllProducts();
                return JsonResponse.DataResponse(result);
            }
            catch (Exception exception)
            {
                return JsonResponse.ErrorResponse(exception.Message);
            }
        }

        [HttpGet("{productId}")]
        public async Task<JsonResponse> GetProductByIdAsync(int productId)
        {
            try
            {
                var result = await productService.RetrieveProductByIdAsync(productId);
                return JsonResponse.DataResponse(result);
            }
            catch (Exception exception)
            {
                return JsonResponse.ErrorResponse(exception.Message);
            }
        }

        [HttpPut("{productId}")]
        public async Task<JsonResponse> PutProductAsync(int productId, ProductForUpdateDTo productDto)
        {
            try
            {
                var result = await productService.ModifyProductAsync(productId, productDto);
                return JsonResponse.DataResponse(result, ResponseMessages.SUCCESS_UPDATE_DATA);
            }
            catch (Exception exception)
            {
                return JsonResponse.ErrorResponse(exception.Message);
            }
        }

        [HttpDelete("{productId}")]
        public async Task<JsonResponse> DeleteProductByIdAsync(int productId)
        {
            try
            {
                var result = await productService.RemoveProductByIdAsync(productId);
                return JsonResponse.DataResponse(result, ResponseMessages.SUCCESS_DELETE_DATA);
            }
            catch (Exception ex)
            {
                return JsonResponse.ErrorResponse(ex.Message);
            }
        }
    }
}