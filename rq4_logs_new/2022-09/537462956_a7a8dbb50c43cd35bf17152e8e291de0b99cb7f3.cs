using Portfolio.Core.ViewModels;
using Portfolio.Data;
using Portfolio.Data.Repositories;

namespace Portfolio.Core.Services
{
    public class ExperienceService : IExperienceService
    {
        private readonly IExperienceRepository _repository;

        public ExperienceService(IExperienceRepository  repository)
        {
            _repository = repository;
        }

        public async Task<List<ExperienceViewModel>> GetAsync()
        {
            var experiences = await _repository.GetAsync();
            return experiences.Select(x => x.ToViewModel()).ToList();
        }
    }
}