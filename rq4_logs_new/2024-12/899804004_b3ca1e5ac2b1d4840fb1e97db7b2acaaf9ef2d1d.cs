using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using ProductSalesDemo.Models;
using ProductSalesDemo.Repository;
using ProductSalesDemo.ViewModel;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace ProductSalesManagement.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductSalesController : ControllerBase
    {
        private readonly IProductSalesRepository _repository;

        //DI - Dependency Injection
        public ProductSalesController(IProductSalesRepository repository)
        {
            _repository = repository;
        }

        #region 1 - Get all Orders - search all
        [HttpGet]
        public async Task<ActionResult<IEnumerable<OrderItem>>> GetAllSalesOrders()
        {
            var orders = await _repository.GetAllOrders();
            if (orders == null)
            {
                return NotFound("No orders found");
            }
            return Ok(orders);
        }
        #endregion

        #region 2 - Get all from viewModel 
        [HttpGet("vm")]
        public async Task<ActionResult<IEnumerable<ProductionSalesViewModel>>> GetAllProductionSalesByViewModel()
        {
            var orders = await _repository.GetOrderViewModel();
            if (orders == null)
            {
                return NotFound("No orders found");
            }
            return Ok(orders);
        }
        #endregion

        #region 3 - Get Orders - Search By Id
        [HttpGet("{id}")]
        public async Task<ActionResult<Order>> GetProdOrderById(int id)
        {
            var order = await _repository.GetOrderById(id);
            if (order == null)
            {
                return NotFound("No orders found");
            }
            return Ok(order);
        }

        #endregion

        #region   4  - Insert an order -return order record
        public async Task<ActionResult<Order>> InsertTblOrdersReturnRecord(Order orderTable)
        {
            if (ModelState.IsValid)
            {
                //insert a new record and return as an object named employee
                var neworder = await _repository.PostTblOrderReturnRecord(orderTable);
                if (neworder != null)
                {
                    return Ok(neworder);
                }
                else
                {
                    return NotFound();
                }
            }
            return BadRequest();
        }
        #endregion

        #region    5 - Insert an order -return Id

        [HttpPost("v1")]
        public async Task<ActionResult<int>> InsertTblOrdersReturnId(Order orderTable)
        {
            if (ModelState.IsValid)
            {
                //insert a new record and return as an object named employee
                var newOrderId = await _repository.PostTblOrderReturnId(orderTable);
                if (newOrderId != null)
                {
                    return Ok(newOrderId);
                }
                else
                {
                    return NotFound();
                }
            }
            return BadRequest();
        }
        #endregion

        #region    6  - Update an order with ID
        [HttpPut("{id}")]
        public async Task<ActionResult<int>> UpdateTblOrdersReturnRecord(int id, Order orderTable)
        {
            if (ModelState.IsValid)
            {
                var updateOrderTable = await _repository.PutTblOrder(id, orderTable);
                if (updateOrderTable != null)
                {
                    return Ok(updateOrderTable);
                }
                else
                {
                    return NotFound();
                }
            }
            return BadRequest();
        }
        #endregion

        #region  7  - Delete an Order
        [HttpDelete("{id}")]
        public IActionResult DeleteOrder(int id)
        {
            try
            {
                var result = _repository.DeleteTblOrder(id);

                if (result == null)
                {
                    //if result indicates failure or null
                    return NotFound(new
                    {
                        success = false,
                        message = "Order could not be deleted or not found"
                    });
                }
                return Ok(result);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError,
                    new { success = false, message = "An unexpected error occurs" });
            }
        }
        #endregion

        #region   8 - Get all OrderItems - search all
        [HttpGet("oi")]
        public async Task<ActionResult<IEnumerable<OrderItem>>> GetAllTblOrderItems()
        {
            var ots = await _repository.GetTblOrderItems();
            if (ots == null)
            {
                return NotFound("No OrderItems found");
            }
            return Ok(ots);
        }
        #endregion

        #region   9 - Get all Customers - search all
        [HttpGet("c1")]
        public async Task<ActionResult<IEnumerable<Customer>>> GetAllTblCustomers()
        {
            var ots = await _repository.GetAllCustomers();
            if (ots == null)
            {
                return NotFound("No Customers found");
            }
            return Ok(ots);
        }
        #endregion

        #region  10  - Get all Brand
        [HttpGet("b1")]
        public async Task<ActionResult<IEnumerable<Brand>>> GetAllTblBrands()
        {
            var ots = await _repository.GetAllBrands();
            if (ots == null)
            {
                return NotFound("No Brands found");
            }
            return Ok(ots);
        }
        #endregion

        #region  11  - Get all Category
        [HttpGet("ca1")]
        public async Task<ActionResult<IEnumerable<Category>>> GetAllTblCategoriess()
        {
            var ots = await _repository.GetAllCategoriess();
            if (ots == null)
            {
                return NotFound("No Categories found");
            }
            return Ok(ots);
        }
        #endregion

        #region   12  - Get all Manager
        [HttpGet("m1")]
        public async Task<ActionResult<IEnumerable<Manager>>> GetAllTblManagers()
        {
            var ots = await _repository.GetAllManagers();
            if (ots == null)
            {
                return NotFound("No Managers found");
            }
            return Ok(ots);
        }
        #endregion

        #region   13  - Get all Products
        [HttpGet("p1")]
        public async Task<ActionResult<IEnumerable<Product>>> GetAllTblProducts()
        {
            var ots = await _repository.GetAllProducts();
            if (ots == null)
            {
                return NotFound("No Products found");
            }
            return Ok(ots);
        }
        #endregion

        #region   14  - Get all Staff
        [HttpGet("staff")]
        public async Task<ActionResult<IEnumerable<Staff>>> GetAllTblStaff()
        {
            var ots = await _repository.GetAllStaff();
            if (ots == null)
            {
                return NotFound("No Staffs found");
            }
            return Ok(ots);
        }
        #endregion

        #region  15  - Get all Stock
        [HttpGet("stock")]
        public async Task<ActionResult<IEnumerable<Stock>>> GetAllTblStock()
        {
            var ots = await _repository.GetAllStock();
            if (ots == null)
            {
                return NotFound("No Stocks found");
            }
            return Ok(ots);
        }
        #endregion

        #region   16  - Get all Store
        [HttpGet("store")]
        public async Task<ActionResult<IEnumerable<Store>>> GetAllTblStores()
        {
            var ots = await _repository.GetAllStores();
            if (ots == null)
            {
                return NotFound("No Stores found");
            }
            return Ok(ots);
        }
        #endregion
    }
}