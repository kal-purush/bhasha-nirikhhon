using System.Net.Sockets;
using System.Net;
using System.Text;


ServerChat server = new ServerChat();
server.Start();

class ServerChat
{
    const short port = 4040;
    const string JOIN_CMD = "$<join>";
    UdpClient server;
    List<IPEndPoint> members;
    IPEndPoint clientEndPoint = null;
    public ServerChat()
    {
        server = new UdpClient(port);
        members = new List<IPEndPoint>();
    }
    private void AddMember()
    {
        members.Add(clientEndPoint);
        Console.WriteLine("Member was added!");
    }
    private void SendToAll(byte[] data)
    {
        foreach (var member in members)
        {
            server.SendAsync(data, data.Length, member);
        }
    }
    public void Start()
    {
        while (true)
        {
            byte[] data = server.Receive(ref clientEndPoint);
            string message = Encoding.UTF8.GetString(data);
            Console.WriteLine($"{message} from {clientEndPoint}. Date: {DateTime.Now}");
            switch (message)
            {
                case JOIN_CMD:
                    AddMember();
                    break;
                default:
                    SendToAll(data);
                    break;
            }
        }
    }

}