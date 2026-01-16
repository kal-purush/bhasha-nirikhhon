using MessageBroker.Server.Models;
using MongoDB.Driver;

namespace MessageBroker.Server.MongoDataAccess;

public class MessagesRepository
{
    private readonly MongoDbContext _context;
    public MessagesRepository(MongoDbContext context)
    {
        _context = context;
    }

    public async Task<List<Message>> GetMessagesAsync(CancellationToken cancellationToken)
    {
        return await _context.Messages.Find(_ => true).ToListAsync(cancellationToken);
    }

    // public async Task<List<Message>> GetMessagesByUserIdAsync(Guid userId, CancellationToken cancellationToken)
    // {
    //     return await _dbContext.Messages.Find(m => m.ChatId).ToListAsync(cancellationToken);
    // }

    public async Task<Guid> Add(Message message, CancellationToken cancellationToken)
    {
        await _context.Messages.InsertOneAsync(message, cancellationToken);
        
        return message.Id;
    }
    
}