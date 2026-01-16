using Business.Abstract;
using Core.Entities.Utilities.Result;
using Entity.Concrete;
using ErroHandler.Constans;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CompanyA.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CompleteOrderController : ControllerBase
    {
        private IOrderService _orderService;
        private readonly ILogger<CompleteOrderController> _logger;
        public CompleteOrderController(ILogger<CompleteOrderController> logger, IOrderService orderService)
        {
            _logger = logger;

            _orderService = orderService;
        }
        [HttpPut]
        [Route("SiparisTammla")]
        public IActionResult SiparisTammla(Order orders)
        {
            try
            {
                if (orders.TeslimAlacakKisi == null)
                {
                    return BadRequest();
                }
                var order = _orderService.GetSiparisTamala(orders.Id);

                if (order.Data != null)
                {
                    order.Data.status = 2;
                    _orderService.Update(order.Data);
                    _logger.LogInformation("Başarılı : " + order.Data.TeslimAlacakKisi, DateTime.UtcNow);
                    return Ok(new SuccesDataResult<Order>(order.Data, true, Message.GetUserSucces));
                    //return Ok(new SuccesResult(true, token));
                }
                else
                {
                    _logger.LogInformation(WarningErrorMessage.GetNoData + "hata", DateTime.UtcNow);
                    return BadRequest(new ErrorDataResult<Users>(WarningErrorMessage.GetNoData));
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Hata");
                throw;
            }
        }
    }
}