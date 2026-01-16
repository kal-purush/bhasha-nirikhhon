using DirectoryProject.DirectoryService.Application.DepartmentHandlers.CreateDepartment;
using DirectoryProject.DirectoryService.Application.LocationHandlers.CreateLocation;
using DirectoryProject.DirectoryService.Application.Shared.DTOs;
using DirectoryProject.DirectoryService.Application.Shared.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace DirectoryProject.DirectoryService.WebAPI.Controllers;

public class LocationsController : ApplicationController
{
    [HttpPost]
    public async Task<IActionResult> Create(
        [FromServices] ICommandHandler<CreateLocationCommand, LocationDTO> handler,
        [FromBody] CreateLocationCommand command,
        CancellationToken cancellationToken = default)
    {
        return ToAPIResponse(await handler.HandleAsync(command, cancellationToken));
    }
}