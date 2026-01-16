using SFA.DAS.Tools.Servicebus.Support.Infrastructure.Services;
using System.Threading.Tasks;

namespace SFA.DAS.Tools.Servicebus.Support.Application.Queue.Commands.DeleteQueueMessage
{
    public class DeleteQueueMessagesCommandHandler : ICommandHandler<DeleteQueueMessagesCommand, DeleteQueueMessagesCommandResponse>
    {
        private readonly ICosmosDbContext _cosmosDbContext;

        public DeleteQueueMessagesCommandHandler(ICosmosDbContext cosmosDbContext)
        {
            _cosmosDbContext = cosmosDbContext;
        }

        public async Task<DeleteQueueMessagesCommandResponse> Handle(DeleteQueueMessagesCommand query)
        {
            await _cosmosDbContext.DeleteQueueMessagesAsync(query.Ids);

            return new DeleteQueueMessagesCommandResponse();
        }
    }
}