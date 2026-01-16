using System.Linq;
using System.Threading.Tasks;
using LT.DigitalOffice.Broker.Requests;
using LT.DigitalOffice.Broker.Responses;
using LT.DigitalOffice.Kernel.Broker;
using LT.DigitalOffice.UserService.Data.Interfaces;
using LT.DigitalOffice.Broker.Models;
using MassTransit;
using Microsoft.AspNetCore.Mvc;

namespace LT.DigitalOffice.UserService.Broker.Consumers
{
    /// <summary>
    /// Consumer for getting information about the users.
    /// </summary>
    public class GetUsersDataConsumer : IConsumer<IGetUsersDataRequest>
    {
        private readonly IUserRepository _repository;

        public GetUsersDataConsumer(IUserRepository repository)
        {
            _repository = repository;
        }

        public async Task Consume(ConsumeContext<IGetUsersDataRequest> context)
        {
            var response = OperationResultWrapper.CreateResponse(GetUserInfo, context.Message);

            await context.RespondAsync<IOperationResult<IGetUsersDataResponse>>(response);
        }

        private object GetUserInfo(IGetUsersDataRequest request)
        {
            var dbUsers = _repository.Get(request.UserIds);

            return IGetUsersDataResponse.CreateObj(
                dbUsers
                    .Select(dbUser => UserData
                        .Create(dbUser.Id, dbUser.FirstName, dbUser.MiddleName, dbUser.LastName, dbUser.IsActive)).ToList());
        }
    }
}