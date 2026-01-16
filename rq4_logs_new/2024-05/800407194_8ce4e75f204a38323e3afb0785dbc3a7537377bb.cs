using Chess;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Windows.Forms;
using System.Xml;
namespace ChessAI
{
    public partial class ChessAIClient : Form
    {
        BoardRenderer boardRenderer;
        ChessBoard chessBoard;
        PieceColor Side;

        //connections
        private TcpClient client;
        private NetworkStream stream;
        private Thread receiveThread;
        //

        public ChessAIClient()
        {
            InitializeComponent();
            DoubleBuffered = true; // remove the small blinking when Invalidate()

            ///Set the side
            Side = PieceColor.White;

            chessBoard = new ChessBoard() { AutoEndgameRules = AutoEndgameRules.All }; // init new board
            boardRenderer = new BoardRenderer(); // init new renderer

            this.Size = new Size(1000, 800); // add minimun offset is 75 and maybe some space
        }
        public void boardPaint(object sender, PaintEventArgs e) // update continously
        {
            boardRenderer.DrawBoard(e.Graphics, chessBoard, 500, isDebug: true, side: Side); // draw the board
            richTextBox1.Text = chessBoard.ToPgn(); // show the moves       
        }

        public void boardClick(object sender, MouseEventArgs e)
        {
            Debug.WriteLine("X: " + e.X + " Y: " + e.Y);
            chessBoard = boardRenderer.onClicked(new Position(e.X, e.Y), chessBoard, isNormalized: false, side: Side); // handle the click
            Invalidate(); // redraw whole screen
        }

        private void button1_Click(object sender, EventArgs e)
        {
            /*var moves = textBox1.Text.Split(' ');
            foreach (var m in moves)
            {
                chessBoard = boardRenderer.onClicked(new Position(m), chessBoard, isNormalized: true); // handle the click
                Invalidate(); // redraw whole screen
            }*/
            if (chessBoard.Turn == Side) return; // simulate opoonent
            var moves = chessBoard.Moves();
            if (chessBoard.IsEndGame)
            {
                Debug.WriteLine("Game end " + chessBoard.EndGame.WonSide + " won!");
                return;
            };
            chessBoard.Move(moves[Random.Shared.Next(moves.Length)]);
            Invalidate();
        }

        private void ClientForm_Load(object sender, EventArgs e)
        {
            try // Attempt to connect to server
            {
                ConnectToServer();
                receiveThread = new Thread(new ThreadStart(ReceiveMessages));
                receiveThread.IsBackground = true;
                receiveThread.Start();
            }
            catch (Exception ex)
            {
                LogMessage("Error: " + ex.Message);
            }
        }

        private void ConnectToServer()
        {

            client = new TcpClient();
            client.Connect("127.0.0.1", 9099);
            stream = client.GetStream();
            LogMessage("Connected to server.");

        }

        private void ReceiveMessages()
        {
            try
            {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while (true)
                {
                    bytesRead = 0;
                    try
                    {
                        bytesRead = stream.Read(buffer, 0, buffer.Length);
                    }
                    catch
                    {
                        break;
                    }

                    if (bytesRead == 0)
                    {
                        break;
                    }

                    string message = Encoding.ASCII.GetString(buffer, 0, bytesRead);
                    LogMessage(message);
                }
            }
            catch (Exception ex)
            {
                LogMessage("Error: " + ex.Message);
            }
        }

        private void SendMessage(string message)
        {
            try
            {
                if (client != null && client.Connected)
                {
                    byte[] data = Encoding.ASCII.GetBytes(message);
                    stream.Write(data, 0, data.Length);
                    LogMessage(message);
                }
                else
                {
                    LogMessage("Not connected to server.");
                }
            }
            catch (Exception ex)
            {
                LogMessage("Error: " + ex.Message);
            }
        }

        private void LogMessage(string message)
        {
            if (InvokeRequired)
            {
                Invoke(new Action<string>(LogMessage), message);
                return;
            }
            richTextBox2.AppendText(message + "\n");
        }

        private void ClientForm_FormClosed(object sender, FormClosedEventArgs e)
        {
            // Send last message to server
            SendMessage("Client disconnected");
            // Close the stream
            if (stream != null)
            {
                stream.Close();
            }
            // Close the client
            if (client != null)
            {
                client.Close();
            }
        }

        private void cntSvr_Click(object sender, EventArgs e)
        {
            try
            {
                ConnectToServer();
                receiveThread = new Thread(new ThreadStart(ReceiveMessages));
                receiveThread.IsBackground = true;
                receiveThread.Start();
            }
            catch (Exception ex)
            {
                LogMessage("Error: " + ex.Message);
            }
        }

    }


}
}