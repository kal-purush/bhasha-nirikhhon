using System.Linq;
using BuddyDomain.Battle.Entities;
using BuddyDomain.Battle.Repositories;

namespace BuddyDomain.Battle.Services
{
    public class PartyAliveService
    {
        private readonly IPartyActorsBuilder partyActorsBuilder;

        public PartyAliveService(IPartyActorsBuilder partyActorsBuilder)
        {
            this.partyActorsBuilder = partyActorsBuilder;
        }

        public bool CheckWipedOut(Party party)
        {
            party.NotifyMember(partyActorsBuilder);
            var actors = partyActorsBuilder.Fetch();
            bool someoneAlive = actors.Any(actor => actor.Alive);
            return !someoneAlive;
        }
    }
}