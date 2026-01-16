using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using SNOW.SHOP.API.Data;
using SNOW.SHOP.API.API.Response;
using SNOW.SHOP.API.API.ViewModels;
using SNOW.SHOP.API.API.Extentions;
using Microsoft.EntityFrameworkCore;
using SNOW.SHOP.API.Model;

namespace SNOW.SHOP.API.Controllers
{
    [Route("api/[controller]")]
    public class ProductController : Controller
    {

        private ISnowShopAPIRepository SnowShopAPIRepository;


        public ProductController(ISnowShopAPIRepository repository)
        {
            SnowShopAPIRepository = repository;
        }

        protected override void Dispose(Boolean disposing)
        {
            SnowShopAPIRepository?.Dispose();

            base.Dispose(disposing);
        }

        // GET Production/Product
        /// <summary>
        /// Retrieves a list of Products
        /// </summary>
        /// <param name="pageSize">Page size</param>
        /// <param name="pageNumber">Page number</param>
        /// <param name="name">Name</param>
        /// <returns>List response</returns>
        [HttpGet]
        [Route("Products")]
        public async Task<IActionResult> GetProducts(Int32? pageSize = 10, Int32? pageNumber = 1, String name = null)
        {
            var response = new ListModelResponse<ProductViewModel>() as IListModelResponse<ProductViewModel>;

            try
            {
                response.PageSize = (Int32)pageSize;
                response.PageNumber = (Int32)pageNumber;

                response.Model = await SnowShopAPIRepository
                        .GetProducts(response.PageSize, response.PageNumber, name)
                        .Select(item => item.ToViewModel())
                        .ToListAsync();

                response.Message = String.Format("Total of records: {0}", response.Model.Count());
            }
            catch (Exception ex)
            {
                response.DidError = true;
                response.ErrorMessage = ex.Message;
            }

            return response.ToHttpResponse();
        }

        // GET Production/Product/5
        /// <summary>
        /// Retrieves a specific Product by id
        /// </summary>
        /// <param name="id">ProductID</param>
        /// <returns>Single response</returns>
        [HttpGet("{id}")]
        public async Task<IActionResult> GetProduct(Int32 id)
        {
            var response = new SingleModelResponse<ProductViewModel>() as ISingleModelResponse<ProductViewModel>;

            try
            {
                var entity = await SnowShopAPIRepository.GetProductAsync(new Product { Id = id });

                response.Model = entity.ToViewModel();
            }
            catch (Exception ex)
            {
                response.DidError = true;
                response.ErrorMessage = ex.Message;
            }

            return response.ToHttpResponse();
        }

        // POST Production/Product/
        /// <summary>
        /// Creates a new user on Production catalog
        /// </summary>
        /// <param name="value">Product entry</param>
        /// <returns>Single response</returns>
        [HttpPost]
        [Route("Product")]
        public async Task<IActionResult> CreateProduct([FromBody]ProductViewModel value)
        {
            var response = new SingleModelResponse<ProductViewModel>() as ISingleModelResponse<ProductViewModel>;

            try
            {
                var entity = await SnowShopAPIRepository.AddProductAsync(value.ToEntity());

                response.Model = entity.ToViewModel();
                response.Message = "The data was saved successfully";
            }
            catch (Exception ex)
            {
                response.DidError = true;
                response.ErrorMessage = ex.ToString();
            }

            return response.ToHttpResponse();
        }

        // PUT Production/Product/5
        /// <summary>
        /// Updates an existing Product
        /// </summary>
        /// <param name="value">Product entry</param>
        /// <returns>Single response</returns>
        [HttpPut]
        [Route("Product")]
        public async Task<IActionResult> UpdateProduct([FromBody]ProductViewModel value)
        {
            var response = new SingleModelResponse<ProductViewModel>() as ISingleModelResponse<ProductViewModel>;

            try
            {
                var entity = await SnowShopAPIRepository.UpdateProductAsync(value.ToEntity());

                response.Model = entity.ToViewModel();
                response.Message = "The record was updated successfully";
            }
            catch (Exception ex)
            {
                response.DidError = true;
                response.ErrorMessage = ex.Message;
            }

            return response.ToHttpResponse();
        }

        // DELETE Production/Product/5
        /// <summary>
        /// Delete an existing Product
        /// </summary>
        /// <param name="id">Product ID</param>
        /// <returns>Single response</returns>
        [HttpDelete]
        [Route("Product/{id}")]
        public async Task<IActionResult> DeleteProduct(Int32 id)
        {
            var response = new SingleModelResponse<ProductViewModel>() as ISingleModelResponse<ProductViewModel>;

            try
            {
                var entity = await SnowShopAPIRepository.DeleteProductAsync(new Product { Id = id });

                response.Model = entity.ToViewModel();
                response.Message = "The record was deleted successfully";
            }
            catch (Exception ex)
            {
                response.DidError = true;
                response.ErrorMessage = ex.Message;
            }

            return response.ToHttpResponse();
        }
    }
}