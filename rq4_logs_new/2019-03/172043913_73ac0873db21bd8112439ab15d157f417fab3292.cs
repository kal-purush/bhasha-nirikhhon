using System.Collections.Generic;
using Integratieproject1.Domain.Ideations;
using Integratieproject1.Domain.Projects;

namespace Integratieproject1.DAL.Interfaces
{
    public interface IIdeationsRepository
    {
        IEnumerable<Ideation> GetIdeations(Phase phase);
        Ideation GetIdeation(int ideationId);
        Ideation CreateIdeation(Ideation ideation);
        IEnumerable<Idea> GetIdeas(Ideation ideation);
        Idea GetIdea(int ideaId);
        Idea CreateIdea(Idea idea);
        IEnumerable<Reaction> GetReactionsOnIdeation(Ideation ideation);
        IEnumerable<Reaction> GetReactionsOnIdea(Idea idea);
        Reaction GetReaction(int reactionId);
        Reaction CreateReaction(Reaction reaction);
        IEnumerable<Vote> GetVotes();
        Vote GetVote(int voteId);
        Vote CreateVote(Vote vote);
    }
}