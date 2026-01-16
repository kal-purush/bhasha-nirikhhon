using AnimalShelter.Core.Models;
using AnimalShelter.DataAccess.Repositories;

namespace AnimalShelter.Application.Services
{
    public class AdoptionApplicationsService : IAdoptionApplicationsService
    {
        private readonly IAdoptionApplicationsRepository _adoptionApplicationsRepository;
        public AdoptionApplicationsService(IAdoptionApplicationsRepository adoptionApplicationRepository)
        {
            _adoptionApplicationsRepository = adoptionApplicationRepository;
        }

        public async Task<List<AdoptionApplication>> GetAllAdoptionApplications()
        {
            return await _adoptionApplicationsRepository.Get();
        }

        public async Task<Guid> CreateAdoptionApplication(AdoptionApplication adoptionApplication)
        {
            return await _adoptionApplicationsRepository.Create(adoptionApplication);
        }

        public async Task<Guid> UpdateAdoptionApplication(
            Guid id,
            DateOnly applicationDate,
            string comment,
            Guid userId,
            Guid animalId,
            Guid statusAdoptionId)
        {
            return await _adoptionApplicationsRepository.Update(
                id, applicationDate, comment, userId, animalId, statusAdoptionId);
        }

        public async Task<Guid> DeleteAdoptionApplication(Guid id)
        {
            return await _adoptionApplicationsRepository.Delete(id);
        }
    }
}