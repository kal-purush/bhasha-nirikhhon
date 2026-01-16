using AskHand.Domain.Climbs;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace AskHand.Infrastructure.Context.Configurations;

public class ClimbConfiguration : IEntityTypeConfiguration<Climb>
{
    public void Configure(EntityTypeBuilder<Climb> builder)
    {
        ConfigureClimbTable(builder);
    }

    private static void ConfigureClimbTable(EntityTypeBuilder<Climb> builder)
    {
        builder.HasKey(e => e.Id);
        builder.Property(e => e.Id)
            .ValueGeneratedNever()
            .HasConversion(
                id => id.Value,
                value => ClimbId.Create(value));

        builder.Property(e => e.EstimationTime);
        builder.Property(e => e.ValidationRate);
    }
}