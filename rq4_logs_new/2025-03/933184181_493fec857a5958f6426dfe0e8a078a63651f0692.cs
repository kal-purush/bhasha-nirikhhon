using MainService.Domain.Entities;
using MainService.Domain.Interfaces;

namespace MainService.Domain.UseCases;

public class ProjectUseCase
{
    private readonly IProjectRepository _projectRepository;

    public ProjectUseCase(IProjectRepository projectRepository)
    {
        _projectRepository = projectRepository;
    }

    public async Task<ProjectDomain> CreateUser(ProjectDomain project)
    {
        // logic business

        // Save the user
        return await _projectRepository.CreateProject(project);
    }
}