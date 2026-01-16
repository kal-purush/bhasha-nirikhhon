using System.Net;
using System.Net.Sockets;
using System.Text;

namespace RD_Client
{
    public partial class Client : Form
    {
        private enum dataFor { Init = 1, Auth, Handle, Stop };
        private readonly IPAddress remoteIP;
        private readonly int remotePort;
        private readonly string password;
        private TcpClient client;
        private NetworkStream stream;
        private bool isConnected;
        internal static bool isOn;

        public Client(IPAddress _remoteIP, int _remotePort, string _password)
        {
            InitializeComponent();
            remoteIP = _remoteIP;
            remotePort = _remotePort;
            password = _password;
            isOn = isConnected = false;
        }

        private void Client_Load(object sender, EventArgs e)
        {
            new Thread(new ThreadStart(Running)).Start();
            isOn = true;
        }

        private void Client_FormClosed(object sender, FormClosedEventArgs e)
        {
            try
            {
                isOn = false;
                if (isConnected)
                {
                    byte[] byteInfo = Encoding.ASCII.GetBytes("Quit");
                    byte[] byteSend = AddHeader(byteInfo, (int)dataFor.Stop);
                    stream.Write(byteSend, 0, byteSend.Length);

                    Thread.Sleep(100);
                    stream.Close();
                    client.Close();
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Exception of type: {ex.GetType().Name}.\nMessage: {ex.Message}.");
            }
        }

        private byte[] AddHeader(byte[] info, int type)
        {
            byte[] rt = new byte[info.Length + 1];
            byte[] header = Encoding.ASCII.GetBytes(type.ToString());
            Buffer.BlockCopy(header, 0, rt, 0, 1);
            Buffer.BlockCopy(info, 0, rt, 1, info.Length);
            return rt;
        }

        public int Connecting()
        {
            try
            {
                client = new TcpClient();
                client.Connect(remoteIP, remotePort);
                if (client.Connected)
                {
                    stream = client.GetStream();
                    byte[] byteInfo = Encoding.ASCII.GetBytes(password);
                    byte[] byteSend = AddHeader(byteInfo, (int)dataFor.Init);
                    stream.Write(byteSend, 0, byteSend.Length);
                    if (Authenticating())
                        return 1;
                    else
                        return 0;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Exception of type: {ex.GetType().Name}.\nMessage: {ex.Message}.");
            }
            return -1;
        }

        private bool Authenticating()
        {
            try
            {
                byte[] byteAuth = new byte[4], byteHead = new byte[1];
                string content;
                int type;
                stream.Read(byteHead, 0, 1);
                type = Convert.ToInt32(Encoding.ASCII.GetString(byteHead));
                if (type == (int)dataFor.Auth)
                {
                    stream.Read(byteAuth, 0, 4);
                    content = Encoding.ASCII.GetString(byteAuth);
                    if (content == "1234")
                    {
                        isConnected = true;
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Exception of type: {ex.GetType().Name}.\nMessage: {ex.Message}.");
            }
            return false;
        }

        private void Running()
        {
            try
            {
                byte[] byteHead = new byte[1];
                byte[] byteInfo, byteSend;
                byte[] byteInfoLength = new byte[6];
                string type;
                int infoLength;
                while (true)
                {
                    stream.Read(byteHead, 0, 1);
                    type = Encoding.ASCII.GetString(byteHead);
                    if (type == ((int)dataFor.Handle).ToString())
                    {
                        stream.Read(byteInfoLength, 0, byteInfoLength.Length);
                        infoLength = Convert.ToInt32(Encoding.ASCII.GetString(byteInfoLength));
                        byteInfo = new byte[infoLength];
                        stream.ReadAsync(byteInfo, 0, infoLength);
                        using (MemoryStream ms = new MemoryStream(byteInfo))
                        {
                            pictureBox.Image = Image.FromStream(ms);
                        }
                    }
                    else if (type == ((int)dataFor.Stop).ToString())
                    {
                        isConnected = false;
                        if (isOn)
                        {
                            byteInfo = Encoding.ASCII.GetBytes("Quit");
                            byteSend = AddHeader(byteInfo, (int)dataFor.Stop);
                            stream.Write(byteSend, 0, byteSend.Length);
                        }
                        break;
                    }
                    else
                    {
                        byte[] byteDel = new byte[client.Available];
                        stream.ReadAsync(byteDel, 0, byteDel.Length);
                    }
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Exception of type: {ex.GetType().Name}.\nMessage: {ex.Message}.");
            }
        }
    }
}