using LagDaemon.YAMUD.API;
using LagDaemon.YAMUD.Model.User;
using System.Net.WebSockets;
using System.Security.Claims;
using System.Text;

namespace LagDaemon.YAMUD.WebAPI.Services.ChatService
{
    public class WebSocketHandler
    {

        public static async Task HandleWebSocket(HttpContext context, WebSocket webSocket)
        {
            var buffer = new byte[1024 * 4];
            var socket = webSocket;

            while (socket.State == WebSocketState.Open)
            {
                var result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);

                if (result.MessageType == WebSocketMessageType.Text)
                {
                    var message = Encoding.UTF8.GetString(buffer, 0, result.Count);

                    // Determine the chat room or recipient based on the WebSocket path
                    string path = context.Request.Path.ToString();
                    if (path.StartsWith("/chat/room"))
                    {
                        await HandleRoomChat(context, socket);
                    }
                    else if (path.StartsWith("/chat/private"))
                    {
                        await HandlePrivateChat(context, socket);
                    }
                    else if (path.StartsWith("/chat/group"))
                    {
                        await HandleGroupChat(context, socket);
                    }
                }
                else if (result.MessageType == WebSocketMessageType.Close)
                {
                    break;
                }
            }

            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
        }

        private static async Task HandleRoomChat(HttpContext context, WebSocket webSocket)
        {

        }

        private static async Task HandleGroupChat(HttpContext context, WebSocket webSocket)
        {

        }

        private static async Task HandlePrivateChat(HttpContext context, WebSocket webSocket)
        {

        }


        private static async Task<UserAccount> GetUserFromContext(HttpContext context)
        {
            // Extract user email from JWT claim
            var userEmailClaim = context.User.Claims.FirstOrDefault(c => c.Type == ClaimTypes.Email)?.Value;

            // Use the user email to retrieve the corresponding user record
            var userRepository = context.RequestServices.GetRequiredService<IRepository<UserAccount>>();
            var user = await userRepository.GetSingleAsync(u => u.EmailAddress == userEmailClaim);

            return user;
        }

        private static async Task SendMessageToRoom(UserAccount user, string message)
        {
            // Determine the room the user is in
            var userRoom = user.PlayerState.CurrentLocation; // Assuming user has a property representing the room

            // Send message to all connected clients in the same room
            // You need to implement this method based on your WebSocket handling logic
            // For example, you might maintain a dictionary of connected clients for each room
            // and send the message to all clients in the specified room
        }
    }
}