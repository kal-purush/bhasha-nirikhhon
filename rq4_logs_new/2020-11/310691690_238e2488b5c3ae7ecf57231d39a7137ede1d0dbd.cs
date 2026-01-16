using GGsLib;
using Microsoft.AspNetCore.Mvc;
using System;

namespace GGsAPI.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class LineItemController : ControllerBase
    {
        private readonly ILineItemService _lineItemService;
        public LineItemController(ILineItemService lineItemService)
        {
            _lineItemService = lineItemService;
        }
        [HttpGet("getAll/{orderId}")]
        [Produces("application/json")]
        public IActionResult GetAllLineItems(int orderId)
        {
            try
            {
                return Ok(_lineItemService.GetAllLineItems(orderId));
            }
            catch (Exception)
            {
                return NotFound();
            }
        }
    }
}