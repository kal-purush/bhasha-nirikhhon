using Lego.EV3.Shared;
using Microsoft.AspNet.SignalR;
using Microsoft.AspNet.SignalR.Hubs;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace Lego.EV3.Net.Web.Hubs
{
    public class VoteCounter
    {

        private const int VOTES_COUNTER_RESET_PERIOD = 3000; //in miliseconds

        private readonly static Lazy<VoteCounter> _instance = new Lazy<VoteCounter>(() => new VoteCounter(GlobalHost.ConnectionManager.GetHubContext<VoteDriveHub>().Clients));

        private IHubConnectionContext<dynamic> Clients { get; set; }

        public readonly Dictionary<DriveCommand, int> CurrentVotesCount = new Dictionary<DriveCommand, int>();

        private VoteCounter(IHubConnectionContext<dynamic> clients)
        {

            Clients = clients;
            CurrentVotesCount.Add(DriveCommand.Left, 0);
            CurrentVotesCount.Add(DriveCommand.Forward, 0);
            CurrentVotesCount.Add(DriveCommand.Right, 0);
            CurrentVotesCount.Add(DriveCommand.Back, 0);

            Timer timer = new Timer(VOTES_COUNTER_RESET_PERIOD);
            timer.Elapsed += ResetVoteCounter;
            timer.Enabled = true;
        }

        public static VoteCounter Instance
        {
            get
            {
                return _instance.Value;
            }
        }

        private void ResetVoteCounter(object sender, ElapsedEventArgs e)
        {
            CurrentVotesCount[DriveCommand.Left] = 0;
            CurrentVotesCount[DriveCommand.Forward] = 0;
            CurrentVotesCount[DriveCommand.Right] = 0;
            CurrentVotesCount[DriveCommand.Back] = 0;
            Clients.All.updateVotesCounter(CurrentVotesCount);
        }

        public void UpdateVoteCounter(DriveCommand command)
        {
            if (CurrentVotesCount[command] < Int32.MaxValue)
            {
                CurrentVotesCount[command]++;
            }
            Clients.All.updateVotesCounter(CurrentVotesCount);
        }


        
    }
}