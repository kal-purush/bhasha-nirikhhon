using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Transenvios.Shipping.Api.Domains.CatalogService.ShipmentRoutePage;
using Transenvios.Shipping.Api.Domains.ClientService.ClientPage;
using Transenvios.Shipping.Api.Domains.RoutesService.RoutesPage;
using Transenvios.Shipping.Api.Domains.UserService.UserPage;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Transenvios.Shipping.Api.Adapters.RoutesService.OrderPage
{
    
    [Route("api/[controller]")]
    [ApiController]
    public class ShipmentRouteController : ControllerBase
    {

        private readonly RoutesProcessor _routeProcessor;

        public ShipmentRouteController(
          RoutesProcessor routesProcessor
           )
        {
            _routeProcessor = routesProcessor ?? throw new ArgumentNullException(nameof(routesProcessor));
        }
        // GET: api/<ClientsController>
        [HttpGet]
        public async Task<ActionResult<IList<ShipmentRoute>>> GetAllAsync()
        {
            var client = await _routeProcessor.GetAllAsync();
            return Ok(client);
        }

        // POST api/<ShipmentRouteController>
        [AllowAnonymous, HttpPost] // sign-up
        public async Task<ActionResult<RouteStateResponse>> RegisterAsync(RouteCreateUpdateRequest model)
        {
            var response = await _routeProcessor.RegisterAsync(model);
            return Ok(response);
        }

  

        //// PUT api/<ShipmentRouteController>/5 /*AllowAnonymous,*/
        //[HttpPut("{id}")]
        //public async Task<ActionResult<RouteStateResponse>> UpdateAsync(Guid id, RouteCreateUpdateRequest model)
        //{
        //    //var response = await _routeProcessor.UpdateAsync(id, model);
        //    //return Ok(response);
        //    return Ok();

        //}
        [HttpPut("{id}")]
        public async Task<ActionResult<ClientStateResponse>> UpdateAsync(Guid id, RouteCreateUpdateRequest model)
        {
            var response = await _routeProcessor.UpdateAsync(id, model);
            return Ok(response);
        }

        // DELETE api/<ShipmentRouteController>/5
        [HttpDelete("{id}")]
        public async Task<ActionResult<RouteStateResponse>> DeleteAsync(Guid id)
        {
            var response = await _routeProcessor.DeleteAsync(id);
            return Ok(response);
        }
        
    }
}