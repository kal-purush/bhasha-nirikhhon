using Microsoft.AspNetCore.Mvc;
using ProductAPI.Modules;
using ProductAPI.Services;
using System.Diagnostics;
using System.Text.Json;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace ProductAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductController : ControllerBase
    {
        private readonly ILogger<ProductController> _logger;
        private readonly IProductDetailsService _productDetailsService;

        public ProductController(ILogger<ProductController> logger, IProductDetailsService productService)
        {
            _logger = logger;
            _productDetailsService = productService;
        }

        // GET: api/<ProductController>
        [HttpGet]
        public string Get()
        {
            ResponseModel responseModel = new ResponseModel();
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            try
            {
                _logger.LogInformation("Get all Product details Api initiated at " + DateTime.UtcNow.ToString());                
                List<product_details> product_Details = _productDetailsService.GetProductList();
                string productDetatilsJsonString = JsonSerializer.Serialize(product_Details);
                return productDetatilsJsonString;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                responseModel.IsSuccess = false;
                responseModel.Messsage = ex.Message;
            }
            finally
            {
                stopwatch.Stop();
                _logger.LogInformation("Get all product details Api executed in {0} ms", stopwatch.ElapsedMilliseconds);
            }
            return JsonSerializer.Serialize(responseModel);
        }

        // GET api/<ProductController>/5
        [HttpGet("{id}")]
        public string Get(int id)
        {
            ResponseModel responseModel = new ResponseModel();
            _logger.LogInformation("Get Product details By ID Api initiated at " + DateTime.UtcNow.ToString());
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            try
            {
                product_details product_Details = _productDetailsService.GetProductDetailsById(id);
                string productDetatilsJsonString = JsonSerializer.Serialize(product_Details);
                return productDetatilsJsonString;
            }
            catch(Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                responseModel.IsSuccess = false;
                responseModel.Messsage = ex.Message;
            }
            finally
            {
                stopwatch.Stop();
                _logger.LogInformation("Get product details By ID Api executed in {0} ms", stopwatch.ElapsedMilliseconds);
            }
            return JsonSerializer.Serialize(responseModel);
        }

        // POST api/<ProductController>
        [HttpPost]
        public string Post([FromBody] product_details value)
        {
            _logger.LogInformation("Save Product details initiated at " + DateTime.UtcNow.ToString());
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            ResponseModel responseModel = new ResponseModel() { IsSuccess = false, Messsage = "Insert/Update failed because of Null argument" };
            try
            {
                if (value == null)
                {
                    throw new Exception("Null argument for insert/update API");
                }
                responseModel = _productDetailsService.SaveProductDetails(value);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
            finally
            {
                stopwatch.Stop();
                _logger.LogInformation("Save product details Api executed in {0} ms", stopwatch.ElapsedMilliseconds);
            }
            return JsonSerializer.Serialize(responseModel);
        }

        // PUT api/<ProductController>/5
        [HttpPut("{id}")]
        public string Put(int id)
        {
            ResponseModel responseModel = new ResponseModel();
            _logger.LogInformation("Put Product details By ID Api initiated at " + DateTime.UtcNow.ToString());
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            try
            {
                _logger.LogInformation("Put method not implemented");
                throw new NotImplementedException();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                responseModel.IsSuccess = false;
                responseModel.Messsage = ex.Message;
            }
            finally
            {
                stopwatch.Stop();
                _logger.LogInformation("Put product details By ID Api executed in {0} ms", stopwatch.ElapsedMilliseconds);
            }
            return JsonSerializer.Serialize(responseModel);
        }

        // DELETE api/<ProductController>/5
        [HttpDelete("{id}")]
        public string Delete(int id)
        {
            _logger.LogInformation("Delete Product details initiated at " + DateTime.UtcNow.ToString());
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            ResponseModel responseModel = new ResponseModel() { IsSuccess = false, Messsage = "Delete failed because of record not found" };
            try
            {
                if (id <= 0)
                {
                    throw new Exception("Invalid Product ID = " + id.ToString());
                }
                responseModel = _productDetailsService.DeleteProductDetails(id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
            finally
            {
                stopwatch.Stop();
                _logger.LogInformation("Save product details Api executed in {0} ms", stopwatch.ElapsedMilliseconds);
            }
            return JsonSerializer.Serialize(responseModel);
        }
    }
}