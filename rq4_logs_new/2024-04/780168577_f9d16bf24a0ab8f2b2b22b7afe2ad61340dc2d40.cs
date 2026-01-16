using Application.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ConferenceApplicationSystem;

public class ActivityConfiguration: IEntityTypeConfiguration<Activity>
{
    public void Configure(EntityTypeBuilder<Activity> builder)
    {
        builder.Property(activity => activity.Type).HasConversion<string>().IsRequired().HasMaxLength(100);;
        builder.Property(activity => activity.Description).IsRequired().HasMaxLength(300);;

    }
}