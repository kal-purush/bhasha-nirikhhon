using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Server.Model;

public class FriendConnection
{
    [Key]
    public Guid ConnectionId { get; init; }
    public Guid SenderId { get; init; }
    [ForeignKey("SenderId")]
    public ApplicationUser Sender { get; set; }
    public Guid ReceiverId { get; init; }
    [ForeignKey("ReceiverId")]
    public ApplicationUser Receiver { get; set; }
    public FriendStatus Status { get; private set; } = FriendStatus.Pending;
    public DateTime SentTime { get; init; } = DateTime.Now;
    public DateTime? AcceptedTime { get; private set; }
    
    public FriendConnection() { }

    public FriendConnection(Guid senderId, Guid receiverId)
    {
        ConnectionId = Guid.NewGuid();
        SenderId = senderId;
        ReceiverId = receiverId;
        Status = FriendStatus.Pending;
        SentTime = DateTime.Now;
        AcceptedTime = null;
    }

    public void SetStatusToAccepted()
    {
        Status = FriendStatus.Accepted;
        AcceptedTime = DateTime.Now;
    }
}