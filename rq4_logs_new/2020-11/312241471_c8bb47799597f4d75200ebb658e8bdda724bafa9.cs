using System.Collections.Generic;
using System.Linq;
using BuddyDomain.Battle.Entities;
using BuddyDomain.Battle.Repositories;
using BuddyDomain.Battle.ValueObjects.Actor;

namespace BuddyDomain.Battle.Services
{
    public class SortBySpeedService
    {
        private readonly IActorRepository actorRepository;

        public SortBySpeedService(IActorRepository actorRepository)
        {
            this.actorRepository = actorRepository;
        }

        public List<ActorId> Sort(IEnumerable<ActorId> actorIds)
        {
            var actors = actorRepository.FindIn(actorIds);
            return actors.OrderByDescending(actor => actor, Actor.SpeedComparer)
                .Select(actor => actor.ActorId).ToList();
        }
    }
}