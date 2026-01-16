using AutoMapper;
using Backend.Domain.Entities;
using Application.Features.ClientProjects.Commands.CreateClientProject;
using Application.Features.ClientProjects.Commands.UpdateClientProject;
using Application.Features.ClientProjects.Queries.GetAllClientProjects;
using Application.Features.ProjectStatuses.Commands.CreateProjectStatus;

namespace Application.AutoMapperProfiles;
public class ClientProjectsAutoMapper : Profile
{
    public ClientProjectsAutoMapper()
    {   
        //GetAllClientProjects
        CreateMap<ClientProject, GetAllClientProjectsResponse>();

        //UpdateClientProject
        CreateMap<UpdateClientProjectRequest, ClientProject>();

        //CreateClientProject
        CreateMap<CreateClientProjectRequest, ClientProject>();

        //CreateProjectStatus
        CreateMap<CreateProjectStatusRequest, ProjectStatus>();
    }
}