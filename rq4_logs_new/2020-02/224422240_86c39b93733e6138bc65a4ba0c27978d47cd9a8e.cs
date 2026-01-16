using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Timers;
using Common;
namespace TypeRacers.Client
{
    public class TypeRacersClient
    {

        private TcpClient client;
        private NetworkStream stream;
        private List<Tuple<string, Tuple<string, string, int>>> opponents;

        public delegate void OpponentsChangedEventHandler(Tuple<List<Tuple<string, Tuple<string, string, int>>>, Dictionary<string, Tuple<bool, int>>> opponentsAndRanking);

        public event OpponentsChangedEventHandler OpponentsChanged;

        public DateTime WaitingTime { get; set; }
        public Dictionary<string, Tuple<bool, int>> Rank { get; set; }
        public string LocalPlayerProgress { get; set; } = "0&0";
        public DateTime PlayersStartingTime { get; set; }
        public DateTime PlayersEndingTime { get; private set; }
        public string Name { get; set; }
        private string ClientInfo { get; set; }

        public TypeRacersClient()
        {
            client = new TcpClient("localhost", 80);
            stream = client.GetStream();

            opponents = new List<Tuple<string, Tuple<string, string, int>>>();
            Rank = new Dictionary<string, Tuple<bool, int>>();
        }

        public void StartServerCommunication()
        {
            Thread thread = new Thread(() =>
            {
                while (true)
                {
                    Thread.Sleep(2000);
                    SetOpponents(new Tuple<List<Tuple<string, Tuple<string, string, int>>>, Dictionary<string, Tuple<bool, int>>>(GetOpponentsProgress(), Rank));
                }
            });
            thread.Start();
        }

        public void NameClient(string username)
        {
            Name = username;
        }


        public string GetTextToType()
        {
            //connecting to server
            ClientInfo = LocalPlayerProgress + "$" + Name + "#";
            var receivedData = SendDataToServer(ClientInfo);

            var dataWithoutHashtag = receivedData.Remove(receivedData.Length - 1);
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
            var dataFromServer = SendDataToServer(ClientInfo);

            var currentOpponents = dataFromServer.Split('%').ToList();
            currentOpponents.Remove("#");
            SetInfoOrRanking(currentOpponents);

            return opponents;
        }

        public void RestartSearch()
        {
            string toSend = Name + "_restart" + "#";

            var dataFromServer = SendDataToServer(toSend);
            var dataWithoutHashtag = dataFromServer.Remove(dataFromServer.Length - 1);
            WaitingTime = DateTime.Parse(dataWithoutHashtag);
        }

        public void SendProgressToServer()
        {
            //writing the progress to stream
            SendDataToServer(ClientInfo);
            SetOpponents(new Tuple<List<Tuple<string, Tuple<string, string, int>>>, Dictionary<string, Tuple<bool, int>>>(GetOpponentsProgress(), Rank));

            stream.Flush();
        }

        public void RemovePlayerFromRoom()
        {
            //connecting to server
            client = new TcpClient("localhost", 80);
            stream = client.GetStream();

            //writing the progress to stream

            string toSend = Name + "_removed" + "#";

            byte[] bytesToSend = Encoding.ASCII.GetBytes(toSend);
            stream.Write(bytesToSend, 0, bytesToSend.Length);
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

            if (progressInfoAndPlayroomInfo.Length == 3)
            {
                var wpmProgress = progressInfoAndPlayroomInfo[0];
                var carProgress = progressInfoAndPlayroomInfo[1];
                var playroomNumber = Convert.ToInt32(progressInfoAndPlayroomInfo[2]);
                var opponentInfo = new Tuple<string, string, int>(wpmProgress, carProgress, playroomNumber);
                var opponent = new Tuple<string, Tuple<string, string, int>>(nameAndInfos.FirstOrDefault(), opponentInfo);
                if (opponents.Contains(opponent))
                {
                    return;
                }
                opponents.Add(opponent);
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

        private string SendDataToServer(string data)
        {

            try
            {
                byte[] bytesToSend = Encoding.ASCII.GetBytes(data);

                stream.Write(bytesToSend, 0, bytesToSend.Length);

                return ReceiveDataFromServer(client);
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        private string ReceiveDataFromServer(TcpClient tcpClient)
        {
            byte[] inStream = new byte[tcpClient.ReceiveBufferSize];
            int read = stream.Read(inStream, 0, inStream.Length);
            string receivedData = Encoding.ASCII.GetString(inStream, 0, read);

            while (!receivedData[read - 1].Equals('#'))
            {
                read = stream.Read(inStream, 0, inStream.Length);
                receivedData += Encoding.ASCII.GetString(inStream, receivedData.Length, read);
            }

            return receivedData;
        }

        //private void OnTimedEvent(object sender, ElapsedEventArgs e)
        //{

        //    if (WaitingTime - DateTime.UtcNow < TimeSpan.Zero)
        //    {
        //        elapsedTime = 0;
        //    }
        //    else
        //    {
        //        SetOpponents(new Tuple<List<Tuple<string, Tuple<string, string, int>>>, Dictionary<string, Tuple<bool, int>>>(GetOpponentsProgress(), Rank));
        //    }
        //}

        //private void OnTimedReportProgressEvent(object sender, ElapsedEventArgs e)
        //{
        //    if (PlayersEndingTime - DateTime.UtcNow < TimeSpan.Zero)
        //    {
        //        return;
        //    }
        //    else
        //    {
        //        SendProgressToServer(LocalPlayerProgress);
        //    }
        //}

    }
}