using Codend.Domain.Core.Abstractions;
using Codend.Domain.Entities;
using Codend.Domain.ValueObjects;

namespace Codend.Domain.Core.Events.ProjectTask;

/// <summary>
/// Domain event raised after ProjectTaskDescription has been changed.
/// </summary>
public class ProjectTaskDescriptionEditedEvent : IDomainEvent
{
    public ProjectTaskDescriptionEditedEvent(ProjectTaskDescription description, ProjectTaskId projectTaskId)
    {
        Description = description;
        ProjectTaskId = projectTaskId;
    }

    public ProjectTaskDescription Description { get; set; }
    public ProjectTaskId ProjectTaskId { get; set; }
}