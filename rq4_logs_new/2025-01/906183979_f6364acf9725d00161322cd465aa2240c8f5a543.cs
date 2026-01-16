using Application.Commands.JobbApplicationCommands;
using Application.Dtos;
using Application.Interfaces.RepoInterface;
using Domain.Models;
using MediatR;
using System;
using System.Threading;
using System.Threading.Tasks;

public class CreateJobbApplicationHandler : IRequestHandler<CreateJobbApplicationCommand, JobbApplicationDto>
{
    private readonly IRepository<JobbApplication> _jobbApplicationRepository;

    public CreateJobbApplicationHandler(IRepository<JobbApplication> jobbApplicationRepository)
    {
        _jobbApplicationRepository = jobbApplicationRepository;
    }

    public async Task<JobbApplicationDto> Handle(CreateJobbApplicationCommand request, CancellationToken cancellationToken)
    {
        // Skapa en ny jobbansökan
        var jobbApplication = new JobbApplication
        {
            JobTitle = request.JobTitle,
            CompanyName = request.CompanyName,
            ApplicationDate = DateTime.UtcNow,
            Status = false,
            UserId = Guid.NewGuid()
        };

        // Spara jobbansökan i databasen
        var createdJobbApplication = await _jobbApplicationRepository.CreateAsync(jobbApplication);

        // Returnera en DTO som svar
        return new JobbApplicationDto
        {
            JobTitle = createdJobbApplication.JobTitle,
            CompanyName = createdJobbApplication.CompanyName
        };
    }
}