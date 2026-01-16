using Microsoft.AspNetCore.SignalR;
using System.Threading.Tasks;

public class ChatHub : Hub
{
    public override Task OnConnectedAsync()
    {
        var userId = Context.User?.Claims?.FirstOrDefault(c => c.Type == "userId")?.Value;
        Console.WriteLine($"User connected with ID: {userId}");
        return base.OnConnectedAsync();
    }

    public async Task SendMessage(int senderId, int receiverId, string message)
    {
        Console.WriteLine($"📩 Đã gửi tin nhắn từ {senderId} đến {receiverId}: {message}");

        // Gửi tin nhắn đến client có ID là receiverId
        await Clients.User(receiverId.ToString()).SendAsync("ReceiveMessage", senderId, message);
    }


}