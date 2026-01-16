using LT.DigitalOffice.Kernel.Broker;
using LT.DigitalOffice.Models.Broker.Models;
using LT.DigitalOffice.Models.Broker.Requests.User;
using LT.DigitalOffice.Models.Broker.Responses.Search;
using LT.DigitalOffice.UserService.Data.Interfaces;
using LT.DigitalOffice.UserService.Models.Db;
using MassTransit;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LT.DigitalOffice.UserService.Broker.Consumers
{
    public class SearchUsersConsumer : IConsumer<ISearchUsersRequest>
    {
        private IUserRepository _userRepository;

        private object SearchUsers(string text)
        {
            List<DbUser> users = _userRepository.Search(text).ToList();

            return ISearchResponse.CreateObj(
                users.Select(
                    u => new SearchInfo(u.Id, string.Join(" ", u.LastName, u.FirstName))).ToList());
        }

        public SearchUsersConsumer(
            IUserRepository userRepository)
        {
            _userRepository = userRepository;
        }

        public async Task Consume(ConsumeContext<ISearchUsersRequest> context)
        {
            var response = OperationResultWrapper.CreateResponse(SearchUsers, context.Message.Value);

            await context.RespondAsync<IOperationResult<ISearchResponse>>(response);
        }
    }
}