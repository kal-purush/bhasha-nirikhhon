using CoreService.Application.Models;
using CoreService.Application.Repositories;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System.Net;

namespace CoreService.Application.Features.Command.Role.AddSingleScreenToCurrentRoleById
{
    internal sealed class AddSingleScreenToCurrentRoleByIdCommandHandler(ILogger<AddSingleScreenToCurrentRoleByIdCommandHandler> logger, IUnitOfWork _unitOfWork) : BaseCqrsAndDomainEventHandler<AddSingleScreenToCurrentRoleByIdCommandHandler>(logger), IRequestHandler<AddSingleScreenToCurrentRoleByIdCommandRequest, AddSingleScreenToCurrentRoleByIdCommandResponse>
    {
        public async Task<AddSingleScreenToCurrentRoleByIdCommandResponse> Handle(AddSingleScreenToCurrentRoleByIdCommandRequest request, CancellationToken cancellationToken)
        {
            var response = new AddSingleScreenToCurrentRoleByIdCommandResponse();
            try
            {
                var screenToAdd = await _unitOfWork.ScreenReadRepository.FindByConditionAsNoTracking(s => s.Id == request.ScreenId).FirstOrDefaultAsync(cancellationToken);
                if (screenToAdd is null)
                {
                    LogWarning("Unable to find this screen", request, HttpStatusCode.BadRequest);
                    response.SetForError("Unable to find this screen", HttpStatusCode.BadRequest);
                    return response;
                }
                var roleToUpdate = await _unitOfWork.RoleReadRepository.FindByCondition(r => r.Id == request.RoleId).Include(r => r.Screens).FirstOrDefaultAsync(cancellationToken);
                if (roleToUpdate is null)
                {
                    LogWarning("Unable to find this role", request, HttpStatusCode.BadRequest);
                    response.SetForError("Unable to find this role", HttpStatusCode.BadRequest);
                    return response;
                }
                else if (roleToUpdate.Screens.Count > 0 && roleToUpdate.Screens.Any(s => s.Id == request.ScreenId))
                {
                    LogWarning("This role already has this screen", request, HttpStatusCode.BadRequest);
                    response.SetForError("This role already has this screen", HttpStatusCode.BadRequest);
                    return response;
                }
                var errorMessage = roleToUpdate.AddSingleScreenToCurrentRole(screenToAdd);
                if (errorMessage is not null)
                {
                    LogWarning("Unable to add this screen to this role", errorMessage, request, HttpStatusCode.BadRequest);
                    response.SetForError(errorMessage, HttpStatusCode.BadRequest);
                    return response;
                }
                await _unitOfWork.SaveChangesAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                LogError("Unable to add this screen to this role", ex, request, HttpStatusCode.InternalServerError);
                response.SetForError("Unexpected error happened while adding this screen to this role", HttpStatusCode.InternalServerError);
            }
            return response;
        }
    }
}