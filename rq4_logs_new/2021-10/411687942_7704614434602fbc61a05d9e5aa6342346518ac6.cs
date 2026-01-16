using System.Collections.Generic;
using System.Threading.Tasks;
using Domain.Events;
using Persistence.Repositories.Classes;

namespace Domain.Services.Class
{
    public class ClassService : IClassService
    {
        private readonly IMessageBus _messageBus;
        private readonly IClassesRepository _repository;
        
        public ClassService(IMessageBus messageBus, IClassesRepository repository)
        {
            _messageBus = messageBus;
            _repository = repository;
        }

        public async Task AddClass(ClassModel model)
        {
            await _repository.AddClass(model);
        }

        public async Task BookClass(BookClassEvent evt)
        {
            await _messageBus.SendClassBookingMessage(evt);
        }

        public async Task<List<ClassReturnModel>> GetClasses(string fitnessName)
        {
            return await _repository.GetClasses(fitnessName);
        }
    }
}