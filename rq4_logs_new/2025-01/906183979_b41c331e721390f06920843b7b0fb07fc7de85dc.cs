
using Application.Dtos;
using Application.Interfaces.RepoInterface;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;



namespace Application.Jobbapplication.JobbApplicationCommands.CreateJobbApplication
{
   

        // Handler
        public class CreateJobbApplicationHandler : IRequestHandler<CreateJobbApplicationCommand, JobbApplicationDto>
        {
            private readonly IRepository<JobbApplicationDto> _jobbApplicationRepository;

            public CreateJobbApplicationHandler(IRepository<JobbApplicationDto> jobbApplicationRepository)
            {
                _jobbApplicationRepository = jobbApplicationRepository;
            }

            public async Task<JobbApplicationDto> Handle(CreateJobbApplicationCommand request, CancellationToken cancellationToken)
            {
                // Map command to entity
                var jobbApplicationEntity = new JobbApplicationDto
                {
                    JobTitle = request.JobTitle,
                    CompanyName = request.CompanyName
                };

                // Save entity to repository
                var createdEntity = await _jobbApplicationRepository.CreateAsync(jobbApplicationEntity);

                // Map entity back to DTO
                return new JobbApplicationDto
                {
                    JobTitle = createdEntity.JobTitle,
                    CompanyName = createdEntity.CompanyName
                };
            }
        }

  
}

