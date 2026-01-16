using Integratieproject1.BL.Models.Ideations;
using Integratieproject1.DAL;
using Integratieproject1.DAL.Repositorys;

namespace Integratieproject1.BL.Managers
{
    public class IdeationsManager
    {
        private CityOfIdeasDbContext ctx;
        private IdeationsRepository ideationsRepository;

        public IdeationsManager()
        {
            ctx = Program.uow.ctx;
            ideationsRepository = new IdeationsRepository(ctx);
        }
        public Ideation GetIdeation(int ideationId)
        {
            return ideationsRepository.GetIdeation(ideationId);
        }
    }
}