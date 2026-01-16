using DotNetify;
using SchedulearnBackend.DataAccessLayer;
using SchedulearnBackend.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SchedulearnBackend.BaseVMs
{
    public class TeamVM : BaseVM
    {
        public class TeamTransfer
        {
            public int UserId { get; set; }
            public int NewTeamId { get; set; }
        }

        private readonly SchedulearnContext _schedulearnContext;
        public Team CurrentTeam { get; set; }
        public Team ManagedTeam { get; set; }

        public IEnumerable<Team> AccessibleTeams
        {
            get
            {
                if (ManagedTeam == null)
                    return new List<Team>();

                return GetAccessibleTeams(ManagedTeam);
            }
        }

        public TeamVM (SchedulearnContext schedulearnContext)
        {
            _schedulearnContext = schedulearnContext;
        }

        public Action<User> SetCurrentUser => user =>
        {
            User currentUser = _schedulearnContext.Users.Where(u => u.Name == user.Name).FirstOrDefault();
            if (currentUser == null)
                throw new Exception("User does not exist");
            CurrentTeam = currentUser.Team;
            ManagedTeam = currentUser.ManagedTeam;

            Changed(nameof(CurrentTeam));
            Changed(nameof(ManagedTeam));
            PushUpdates();
        };

        public Action<Limit> ChangeLimitsForMyTeam => teamLimits =>
        {
            if (CurrentTeam == null)
                throw new Exception("Current team not set");

            Limit wantedLimit = _schedulearnContext.Limits
                .Where(l => l.LimitOfConsecutiveLearningDays == teamLimits.LimitOfConsecutiveLearningDays)
                .Where(l => l.LimitOfLearningDaysPerMonth == teamLimits.LimitOfLearningDaysPerMonth)
                .Where(l => l.LimitOfLearningDaysPerQuarter == teamLimits.LimitOfLearningDaysPerQuarter)
                .Where(l => l.LimitOfLearningDaysPerYear == teamLimits.LimitOfLearningDaysPerYear)
                .FirstOrDefault();

            if (wantedLimit == null)
                throw new Exception("This combination of limits is not possible");

            CurrentTeam.LimitId = wantedLimit.Id;
            _schedulearnContext.SaveChanges();

            Changed(nameof(CurrentTeam));
            PushUpdates();
        };

        public Action<TeamTransfer> TransferUserToNewTeam => transfer =>
        {
            User user = _schedulearnContext.Users.Find(transfer.UserId);
            if (user == null)
                throw new Exception("User does not exist");
            if (_schedulearnContext.Teams.Find(transfer.NewTeamId) == null)
                throw new Exception("Team does not exist");

            user.TeamId = transfer.NewTeamId;
            _schedulearnContext.SaveChanges();

            Changed(nameof(CurrentTeam));
            PushUpdates();
        };

        private IEnumerable<Team> GetAccessibleTeams(Team currentTeam)
        {
            var accessibleTeams = new List<Team>();
            foreach (User member in currentTeam.Members)
            {
                if (member.ManagedTeam != null)
                {
                    accessibleTeams.Add(member.ManagedTeam);
                    accessibleTeams.AddRange(GetAccessibleTeams(member.ManagedTeam));
                }
            }

            return accessibleTeams;
        }
    }
}