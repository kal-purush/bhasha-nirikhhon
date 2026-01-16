using AnimalShelter.Core.Models;
using AnimalShelter.DataAccess.Entities;
using Microsoft.EntityFrameworkCore;

namespace AnimalShelter.DataAccess.Repositories
{
    public class AdoptionApplicationsRepository : IAdoptionApplicationsRepository
    {
        private readonly AnimalShelterDbContext _context;

        public AdoptionApplicationsRepository(AnimalShelterDbContext context)
        {
            _context = context;
        }

        public async Task<List<AdoptionApplication>> Get()
        {
            var adoptionApplicationEntities = await _context.AdoptionApplication
                .AsNoTracking()
                .ToListAsync();

            var adoptionApplications = adoptionApplicationEntities
                .Select(b => AdoptionApplication.Create(b.Id, b.ApplicationDate, b.Comment, b.UserId, b.AnimalId, b.StatusAdoptionId).AdoptionApplication)
                .ToList();

            return adoptionApplications;
        }

        public async Task<Guid> Create(AdoptionApplication adoptionApplication)
        {
            var adoptionApplicationEntity = new AdoptionApplicationEntity
            {
                Id = adoptionApplication.Id,
                ApplicationDate = adoptionApplication.ApplicationDate,
                Comment = adoptionApplication.Comment,
                UserId = adoptionApplication.UserId,
                AnimalId = adoptionApplication.AnimalId,
                StatusAdoptionId = adoptionApplication.StatusAdoptionId
            };

            await _context.AdoptionApplication.AddAsync(adoptionApplicationEntity);
            await _context.SaveChangesAsync();

            return adoptionApplicationEntity.Id;
        }

        public async Task<Guid> Update(
            Guid id,
            DateOnly applicationDate,
            string comment,
            Guid userId,
            Guid animalId,
            Guid statusAdoptionId)
        {
            await _context.AdoptionApplication
                .Where(b => b.Id == id)
                .ExecuteUpdateAsync(s => s
                    .SetProperty(b => b.ApplicationDate, b => applicationDate)
                    .SetProperty(b => b.Comment, b => comment)
                    .SetProperty(b => b.UserId, b => userId)
                    .SetProperty(b => b.AnimalId, b => animalId)
                    .SetProperty(b => b.StatusAdoptionId, b => statusAdoptionId));

            return id;
        }

        public async Task<Guid> Delete(Guid id)
        {
            await _context.AdoptionApplication
                .Where(b => b.Id == id)
                .ExecuteDeleteAsync();

            return id;
        }
    }
}