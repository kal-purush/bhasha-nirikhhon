using DiscordWebSocket.Payloads;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DiscordWebSocket.Controllers
{
    class SocketController
    {
        public static bool IsConnected
        {
            get
            {
                return WebSocket.State == WebSocketState.Open;
            }
        }
        public static ClientWebSocket WebSocket
        {
            get
            {
                if (_WebSocket == null)
                    _WebSocket = new ClientWebSocket();

                return _WebSocket;
            }
        }
        private static ClientWebSocket _WebSocket;
        private static JsonSerializerSettings _Settings
        {
            get
            {
                return new JsonSerializerSettings()
                {
                    NullValueHandling = NullValueHandling.Ignore,
                    MissingMemberHandling = MissingMemberHandling.Ignore
                };
            }
        }

        public static string Serialize(object data)
        {
            return JsonConvert.SerializeObject(data);
        }
        public static T Deserialize<T>(string data)
        {
            return JsonConvert.DeserializeObject<T>(data, _Settings);
        }

        public static async Task<bool> Connect(string url)
        {
            await WebSocket.ConnectAsync(new Uri(url), 
                new CancellationToken());

            return WebSocket.State == WebSocketState.Open;
        }
        public static Payload Send(Payload payload)
        {
            string json = Serialize(payload);
            byte[] buffer = Encoding.UTF8.GetBytes(json);

            WebSocket.SendAsync(new ArraySegment<byte>(buffer),
                WebSocketMessageType.Text,
                true,
                new CancellationToken());

            return Receive();
        }
        public static Payload Receive()
        {
            byte[] buffer = new byte[4096];
            WebSocket.ReceiveAsync(new ArraySegment<byte>(buffer), 
                new CancellationToken());
            
            int lastIndex = buffer.ToList().FindLastIndex(b => b != 0);
            byte[] tempBuffer = new byte[lastIndex+1];
            Buffer.BlockCopy(buffer, 0, tempBuffer, 0, tempBuffer.Length);

            string json = Encoding.UTF8.GetString(tempBuffer);
            Payload payload = Deserialize<Payload>(json);

            return payload;
        }
    }
}