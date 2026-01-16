using AutoMapper;
using CommandService.Data;
using CommandService.Dtos;
using Microsoft.AspNetCore.Mvc;

namespace CommandService.Controllers;

[Route("api/c/platforms/{platformId}/[controller]")]
[ApiController]
public class CommandsController : ControllerBase
{
    private readonly ICommandRepo _repo;
    private readonly IMapper _mapper;

    public CommandsController(ICommandRepo repo, IMapper mapper)
    {
        _repo = repo;
        _mapper = mapper;
    }   
    
    [HttpGet]
    public ActionResult<IEnumerable<CommandReadDto>> GetCommandsForPlatform(int platformId)
    {
        Console.WriteLine("--> Getting Command for platformId from CommandService");
        if (!_repo.PlatformExists(platformId)) return NotFound();
        
        var commands = _repo.GetCommandsForPlatform(platformId);
        return Ok(_mapper.Map<IEnumerable<CommandReadDto>>(commands));
    }
    
}