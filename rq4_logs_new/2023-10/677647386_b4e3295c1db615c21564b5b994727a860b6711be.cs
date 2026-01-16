using Microsoft.AspNetCore.Mvc;
using TsdDelivery.Api.Filters;
using TsdDelivery.Application.Interface.V2;
using TsdDelivery.Application.Models.Service.Request;

namespace TsdDelivery.Api.Controllers.V2;

[Route("api/v{version:apiVersion}/services")] 
[ApiController]
[ApiVersion("2.0")]
public class ServiceController : BaseController
{
    private readonly IService _service;

    public ServiceController(IService service)
    {
        _service = service;
    }
    
    [HttpGet]
    public async Task<IActionResult> GetServices()
    {
        var response = await _service.GetServices();
        return response.IsError ? HandleErrorResponse(response.Errors) : Ok(response.Payload);
    }
    
    [HttpGet("{id}")]
    public async Task<IActionResult> GetService(Guid id)
    {
        var response = await _service.GetService(id);
        return response.IsError ? HandleErrorResponse(response.Errors) : Ok(response.Payload);
    }
    
    [HttpPost]
    [ValidateModel]
    public async Task<IActionResult> CreateService(CreateServiceRequest request)
    {
        var response = await _service.CreateService(request);
        return response.IsError ? HandleErrorResponse(response.Errors) : CreatedAtAction(nameof(GetService), new {id = response.Payload?.Id},response.Payload);
    }
    
    [HttpDelete]
    public async Task<IActionResult> DeleteService(Guid serviceId, Guid vehicleTypeId)
    {
        var response = await _service.DeleteService(serviceId,vehicleTypeId);
        return response.IsError ? HandleErrorResponse(response.Errors) : NoContent();
    }
}