using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace TypeRacers.Client
{
    public class Player
    {
        TcpClient client;
        NetworkStream stream;

        public Player(TcpClient tcpClient)
        {
            this.client = tcpClient;
        }


        public DateTime WaitingTime { get; set; }
        public Dictionary<string, Tuple<bool, int>> Rank { get; set; } = new Dictionary<string, Tuple<bool, int>>();
        public string LocalPlayerProgress { get; set; } = "0&0";
        public DateTime PlayersStartingTime { get; set; }
        public DateTime PlayersEndingTime { get; private set; }

        public string DataToSend { get; set; } = "test";

        public string DataReceived { get; set; }
        public string Name { get; set; }
        public bool GameStarted { get; private set; }

        public int Interval { get; set; } // 1 second

        private List<Tuple<string, Tuple<string, string, int>>> opponents;
        public delegate void TimerTickHandler(Tuple<List<Tuple<string, Tuple<string, string, int>>>, Dictionary<string, Tuple<bool, int>>> opponentsAndRanking);

        public event TimerTickHandler OpponentsChanged;

        public void NameClient(string username)
        {
            Name = username;
        }
        public void StartTimerForSearchingOpponents()
        {
            while (WaitingTime - DateTime.UtcNow >= TimeSpan.Zero)
            {
                Interval = 1000;

                SetOpponents(new Tuple<List<Tuple<string, Tuple<string, string, int>>>, Dictionary<string, Tuple<bool, int>>>(GetOpponentsProgress(), Rank));

            }

        }

        public void StartTimerForGameProgressReports()
        {
            while (PlayersEndingTime - DateTime.UtcNow >= TimeSpan.Zero)
            {
                Interval = 3000;

                SendProgressToServer(LocalPlayerProgress);
            }

        }
        public void SendDataToServer()
        {
            stream = client.GetStream();
            while (true)
            {
                try
                {
                    byte[] bytesToSend = Encoding.ASCII.GetBytes(DataToSend);

                    stream.Write(bytesToSend, 0, bytesToSend.Length);


                    ReceiveDataFromServer(client);

                }
                catch (Exception e)
                {
                    throw e;
                }
            }
        }

        private void ReceiveDataFromServer(TcpClient tcpClient)
        {
            byte[] inStream = new byte[tcpClient.ReceiveBufferSize];
            int read = stream.Read(inStream, 0, inStream.Length);
            DataReceived = Encoding.ASCII.GetString(inStream, 0, read);

            while (!DataReceived[read - 1].Equals('#'))
            {
                read = stream.Read(inStream, 0, inStream.Length);
                DataReceived += Encoding.ASCII.GetString(inStream, DataReceived.Length, read);
            }
        }

        private void SetOpponents(Tuple<List<Tuple<string, Tuple<string, string, int>>>, Dictionary<string, Tuple<bool, int>>> value)
        {
            opponents = value.Item1;
            Rank = value.Item2;
            OnOpponentsChanged(value);
        }

        protected void OnOpponentsChanged(Tuple<List<Tuple<string, Tuple<string, string, int>>>, Dictionary<string, Tuple<bool, int>>> opponents)
        {
            if (opponents != null)
            {
                OpponentsChanged(opponents);
            }
        }

        //receiving the opponents and their progress in a List
        private void SetInfoOrRanking(List<string> currentOpponents)
        {
            foreach (var v in currentOpponents)
            {
                if (v.FirstOrDefault().Equals('*'))
                {
                    var times = v.Substring(1).Split('+');
                    PlayersStartingTime = DateTime.Parse(times.FirstOrDefault());
                    PlayersEndingTime = DateTime.Parse(times.LastOrDefault());
                }
                else
                {
                    if (v.FirstOrDefault().Equals('!'))
                    {
                        var rank = v.Substring(1).Split(';');
                        SetRanking(rank);
                    }
                    else
                    {
                        var nameAndInfos = v.Split(':');
                        SetInfo(nameAndInfos);
                    }
                }
            }
        }

        private void SetInfo(string[] nameAndInfos)
        {
            var progressInfoAndPlayroomInfo = nameAndInfos.LastOrDefault()?.Split('&');

            if (progressInfoAndPlayroomInfo.Count() == 3)
            {
                var wpmProgress = progressInfoAndPlayroomInfo[0];
                var carProgress = progressInfoAndPlayroomInfo[1];
                var playroomNumber = Convert.ToInt32(progressInfoAndPlayroomInfo[2]);
                var opponentInfo = new Tuple<string, string, int>(wpmProgress, carProgress, playroomNumber);
                opponents.Add(new Tuple<string, Tuple<string, string, int>>(nameAndInfos.FirstOrDefault(), opponentInfo));
            }
        }

        private void SetRanking(string[] rank)
        {
            rank.Aggregate(Rank, (Rank, r) =>
            {
                if (!string.IsNullOrEmpty(r))
                {
                    var nameAndRank = r.Split(':');
                    var name = nameAndRank[0];
                    var currentRank = nameAndRank[1].Split('&');
                    var rankToAdd = new Tuple<bool, int>(Convert.ToBoolean(currentRank[0]), Convert.ToInt32(currentRank[1]));

                    if (!Rank.ContainsKey(name))
                    {
                        Rank.Add(name, rankToAdd);
                    }
                    else
                    {
                        Rank[name] = rankToAdd;
                    }
                }
                return Rank;
            });
        }

        public string ConnectingToServer()
        {
            //connecting to server
            DataToSend = LocalPlayerProgress + "$" + Name + "#";

            SendDataToServer();

            var dataWithoutHashtag = DataReceived.Remove(DataReceived.Length - 1);
            var textToType = dataWithoutHashtag.Substring(0, dataWithoutHashtag.IndexOf('$'));

            var times = dataWithoutHashtag.Substring(dataWithoutHashtag.IndexOf('%') + 1);
            var gameTimers = times.Split('*');
            WaitingTime = DateTime.Parse(gameTimers.FirstOrDefault());
            var startAndEndTimes = gameTimers.LastOrDefault().Split('+');
            PlayersStartingTime = DateTime.Parse(startAndEndTimes.FirstOrDefault());
            PlayersEndingTime = DateTime.Parse(startAndEndTimes.LastOrDefault());
            return textToType;
        }

        public List<Tuple<string, Tuple<string, string, int>>> GetOpponentsProgress()
        {
            //connecting to server
            DataToSend = LocalPlayerProgress + "$" + Name + "#";

            SendDataToServer();

            var currentOpponents = DataReceived.Split('%').ToList();
            currentOpponents.Remove("#");

            opponents = new List<Tuple<string, Tuple<string, string, int>>>();
            SetInfoOrRanking(currentOpponents);

            return opponents;

        }

        public void RestartSearch()
        {

            DataToSend = Name + "_restart" + "#";

            SendDataToServer();
            var dataWithoutHashtag = DataReceived.Remove(DataReceived.Length - 1);
            WaitingTime = DateTime.Parse(dataWithoutHashtag);

        }

        public void SendProgressToServer(string progress)
        {

            //writing the progress to stream
            DataToSend = progress + "&" + "$" + Name + "#";

            SendDataToServer();
            SetOpponents(new Tuple<List<Tuple<string, Tuple<string, string, int>>>, Dictionary<string, Tuple<bool, int>>>(GetOpponentsProgress(), Rank));

            stream.Flush();
        }

        public void RemovePlayerFromRoom()
        {
            //writing the progress to stream

            DataToSend = Name + "_removed" + "#";
            SendDataToServer();

        }
    }
}