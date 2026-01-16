using Codend.Domain.Entities;
using Codend.Domain.ValueObjects;
using Codend.Persistence.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Codend.Persistence.Configurations;

/// <summary>
/// Entity framework configuration for the <see cref="ProjectTaskStatus"/> entity.
/// </summary>
internal sealed class ProjectTaskStatusConfiguration : IEntityTypeConfiguration<ProjectTaskStatus>
{
    /// <inheritdoc />
    public void Configure(EntityTypeBuilder<ProjectTaskStatus> builder)
    {
        builder.ConfigureKeyId((Guid guid) => new ProjectTaskStatusId(guid));

        builder.OwnsOne(projectTaskStatus => projectTaskStatus.Name,
            projectTaskStatusNameBuilder =>
            {
                projectTaskStatusNameBuilder.WithOwner();

                projectTaskStatusNameBuilder.Property(projectTaskName => projectTaskName.Name)
                    .HasColumnName(nameof(ProjectTask.Name))
                    .HasMaxLength(ProjectTaskName.MaxLength)
                    .IsRequired();
            });

        builder
            .HasOne(projectTaskStatus => projectTaskStatus.Project);
    }
}