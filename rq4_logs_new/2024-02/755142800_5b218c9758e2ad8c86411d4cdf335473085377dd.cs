using Application.IRepositories;
using Application.IServices;
using Application.Models.Dtos;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers;

public class UserTestController : BaseController
{
    private readonly IUserTestService _userTestService;

    public UserTestController(IUserTestService userTestService)
    {
        _userTestService = userTestService;
    }
    
    [HttpPut("update/testId={testId}/userId={userId}/status={status}")]
    public async Task<ActionResult<UserTestDto>> LoginAsync(Guid testId, Guid userId, bool status,
        CancellationToken cancellationToken)
    {
        var userTest = await _userTestService.UpdateTestCompletedStatus(testId, userId, status, cancellationToken);
        return Ok(userTest);
    }
    
    [HttpPost("create/testId={testId}/userId={userId}")]
    public async Task<ActionResult<UserTestDto>> DetermineTestAsync(Guid testId, Guid userId,
        CancellationToken cancellationToken)
    {
        var userTest = await _userTestService.AddUserTest(testId, userId, cancellationToken);
        return Ok(userTest);
    }
}