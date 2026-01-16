using System.Collections.Generic;
using System.Threading.Tasks;
using HelpI.API.Session.Domain.Models;

namespace HelpI.API.Session.Domain.Persistence.Repositories
{
    public interface IIndividualSessionRepository
    {
        Task<IEnumerable<CoachingSession>> ListAsync();
        Task<CoachingSession> FindById(int id);
        Task AddAsync(CoachingSession session);
        Task<IEnumerable<CoachingSession>> ListByPlayerIdAsync(int playerId);
        Task<IEnumerable<CoachingSession>> ListByExpertIdAsync(int expertId);
        Task<IEnumerable<CoachingSession>> FindByPlayerIdAndExpertId(int playerId, int expertId);
        void Update(CoachingSession session);
        void Remove(CoachingSession session);
    }
}