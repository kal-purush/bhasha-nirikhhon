using CoreService.Application.Models;
using CoreService.Application.Repositories;
using CoreService.Domain.AggregateRoots.User;
using CoreService.Domain.Common;
using CoreService.Domain.DomainEvents.User;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace CoreService.Application.DomainEventHandlers.User
{
    internal class SetProfileAfterUserCreatedEventHandler(ILogger<SetProfileAfterUserCreatedEventHandler> logger, IUnitOfWork unitOfWork) : BaseCqrsAndDomainEventHandler<SetProfileAfterUserCreatedEventHandler>(logger, unitOfWork), IDomainEventHandler<SetProfileAfterUserCreatedEvent>

    {
        public async Task Handle(SetProfileAfterUserCreatedEvent notification, CancellationToken cancellationToken)
        {
            try
            {
                var (profileToCreate, errorMessage) = ProfileEntity.CreateNewProfileEntityWithoutAgeAndBirthDate(notification.Firstname, notification.Lastname, notification.UserId);
                if (profileToCreate == null)
                {
                    LogWarning("Unable to create this profile", errorMessage, HttpStatusCode.BadRequest);
                    return;
                }
                var foundCompany = await _unitOfWork.CompanyReadRepository.FindByConditionAsNoTracking(c => c.Name == notification.CompanyName).FirstOrDefaultAsync(cancellationToken);
                if (foundCompany != null) //So if this company already exist
                {
                    profileToCreate.SetCompanyId(foundCompany.Id);
                }
                else
                {
                    (var companyToCreate, errorMessage) = CompanyEntity.CreateNewCompanyOnlyWithName(notification.CompanyName);
                    if (companyToCreate == null)
                    {
                        LogWarning("Unable to create this company", errorMessage, HttpStatusCode.BadRequest);
                        return;
                    }
                    await _unitOfWork.CompanyWriteRepository.InsertSingleAsync(companyToCreate, cancellationToken);
                    await _unitOfWork.SaveChangesAsync(cancellationToken);
                    profileToCreate.SetCompanyId(companyToCreate.Id);
                }
                await _unitOfWork.ProfileWriteRepository.InsertSingleAsync(profileToCreate, cancellationToken);
                await _unitOfWork.SaveChangesAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                LogError("Unable to create this profile", ex, notification, HttpStatusCode.InternalServerError);
            }
        }
    }
}