using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using HelpI.API.Domain.Models.Session;
using HelpI.API.Domain.Persistence.Repositories;
using HelpI.API.Domain.Persistence.Repositories.Session;
using HelpI.API.Domain.Services.Communications;
using HelpI.API.Domain.Services.Session;

namespace HelpI.API.Application.Services.Session
{
    public class ScheduledSessionService : IScheduledSessionService
    {
        private readonly IScheduledSessionRepository _scheduledSessionRepository;
        private readonly IUnitOfWork _unitOfWork;

        public ScheduledSessionService(IScheduledSessionRepository scheduledSessionRepository, IUnitOfWork unitOfWork)
        {
            _scheduledSessionRepository = scheduledSessionRepository;
            _unitOfWork = unitOfWork;
        }
        
        public async Task<ScheduledSessionResponse> DeleteAsync(int id)
        {
            var existingScheduledSession = await _scheduledSessionRepository.FindById(id);

            if (existingScheduledSession == null)
                return new ScheduledSessionResponse("ScheduledSession Not Found");

            try
            {
                _scheduledSessionRepository.Remove(existingScheduledSession);
                await _unitOfWork.CompleteAsync();
                return new ScheduledSessionResponse(existingScheduledSession);
            }
            catch (Exception ex)
            {
                return new ScheduledSessionResponse($"An error ocurred while saving ScheduledSession: {ex.Message}");
            }
        }

        public async Task<ScheduledSessionResponse> GetByIdAsync(int id)
        {
            var existingScheduledSession = await _scheduledSessionRepository.FindById(id);
            if (existingScheduledSession == null)
                return new ScheduledSessionResponse("ScheduledSession Not Found");
            return new ScheduledSessionResponse(existingScheduledSession);
        }

        public async Task<IEnumerable<ScheduledSession>> ListAsync()
        {
            return await _scheduledSessionRepository.ListAsync();
        }
    }
}