using Microsoft.AspNetCore.Mvc;
using TsdDelivery.Application.Interface;

namespace TsdDelivery.Api.Controllers;

public class DashBoardController : BaseController
{
    private readonly IDashBoardService _dashBoardService;

    public DashBoardController(IDashBoardService dashBoardService)
    {
        _dashBoardService = dashBoardService;
    }

    [HttpGet]
    public async Task<IActionResult> GetCountPercentUser()
    {
        var response = await _dashBoardService.GetCountPercentUser();
        return response.IsError ? HandleErrorResponse(response.Errors) : Ok(response.Payload);
    }
    
    [HttpGet]
    public async Task<IActionResult> GetRevenueDataByTime(DateTime from, DateTime to)
    {
        var response = await _dashBoardService.GetRevenueDataByTime(from,to);
        return response.IsError ? HandleErrorResponse(response.Errors) : Ok(response.Payload);
    }
}