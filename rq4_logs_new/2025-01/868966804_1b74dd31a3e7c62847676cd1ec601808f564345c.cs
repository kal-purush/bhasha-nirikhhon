using digioz.Forum.Models;
using digioz.Forum.Services.Interfaces;
using Microsoft.Build.Framework;


namespace digioz.Forum.Services
{
    public class SessionService : ISessionService
    {
        private DigiozForumContext _context;

        public SessionService(DigiozForumContext context)
        {
            _context = context;
        }

        public ForumSession Get(int id)
        {
            var model = _context.ForumSessions.Find(id);
            return model;
        }

        public IEnumerable<ForumSession> GetAll()
        {
            var models = _context.ForumSessions;
            return models;
        }

        public void Add(ForumSession session)
        {
            _context.ForumSessions.Add(session);
            _context.SaveChanges();
        }

        public void Edit(ForumSession session)
        {
            var model = _context.ForumSessions.Find(session.Id);
            if (model != null)
            {
                _context.ForumSessions.Update(session);
                _context.SaveChanges();
            }
        }

        public void Delete(int id)
        {
            var model = _context.ForumSessions.Find(id);
            if (model != null)
            {
                _context.ForumSessions.Remove(model);
                _context.SaveChanges();
            }
        }
    }
}