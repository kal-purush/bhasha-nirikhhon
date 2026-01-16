using backend_carhelp.Iam.Domain.Model.Aggregates;
using backend_carhelp.Iam.Domain.Model.Commands;

namespace backend_carhelp.Users.Domain.Model.Entities;

public class Notification
{
    public int Id { get; }
    public string Type { get; private set; }
    public string Title { get; private set; }
    public string Message { get; private set; }
    public string Timestamp { get; private set; }
    public bool Read { get; private set; }
    public User User { get; set; }
    public int UserId { get; private set; }
    
    public Notification(string type, string title, string message, string timestamp, bool read, int userId)
    {
        Type = type;
        Title = title;
        Message = message;
        Timestamp = timestamp;
        Read = read;
        UserId = userId;
    }

    public Notification(CreateNotificationCommand command) 
        : this(command.Type, command.Title, command.Message, command.Timestamp, command.Read, command.UserId) {}

}