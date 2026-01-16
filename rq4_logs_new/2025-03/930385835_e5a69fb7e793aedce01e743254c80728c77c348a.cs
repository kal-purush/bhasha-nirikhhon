using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using PetFamily.Discussions.Domain.Entities;
using PetFamily.Discussions.Domain.ValueObjects;
using PetFamily.SharedKernel.ValueObjects.Ids;

namespace PetFamily.Discussions.Infrastructure.Configurations.Write;

public class DiscussionConfiguration : IEntityTypeConfiguration<Discussion>
{
    public void Configure(EntityTypeBuilder<Discussion> builder)
    {
        builder.ToTable("discussions");

        builder.HasKey(d => d.Id);

        builder.Property(d => d.Id)
            .HasConversion(
                id => id.Value,
                value => DiscussionId.Create(value))
            .IsRequired()
            .HasColumnName("id");

        builder.Property(d => d.RelationId)
            .HasConversion(
                id => id.Value,
                value => RelationId.Create(value))
            .IsRequired()
            .HasColumnName("relation_id");

        builder.ComplexProperty(d => d.Status, sb =>
        {
            sb.Property(s => s.Value)
                .IsRequired()
                .HasConversion(
                    status => (int)status,
                    status => (DiscussionStatusEnum)status)
                .HasColumnName("status");
        });

        builder.HasMany(v => v.Messages)
            .WithOne()
            .HasForeignKey("discussion_id")
            .IsRequired()
            .OnDelete(DeleteBehavior.Cascade);

        builder.Navigation(s => s.Messages).AutoInclude();
        
        builder.OwnsMany(
            d => d.UserIds,
            userIdBuilder =>
            {
                userIdBuilder.ToTable("discussion_users");
                userIdBuilder.WithOwner().HasForeignKey("discussion_id");
                userIdBuilder.Property(u => u.Value).HasColumnName("user_id");
            });
    }
}