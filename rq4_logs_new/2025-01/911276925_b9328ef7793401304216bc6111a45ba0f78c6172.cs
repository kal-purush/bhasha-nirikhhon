using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using TShort.Api.Data.Models;

namespace TShort.Api.Data.Configuration;

public sealed class RedirectConfiguration : IEntityTypeConfiguration<Redirect>
{
    public void Configure(EntityTypeBuilder<Redirect> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.HasKey(r => r.ShortName);
        builder.HasIndex(r => r.CreatedBy);

        builder.Property(r => r.ShortName)
            .HasMaxLength(50)
            .IsRequired();

        builder.Property(r => r.RedirectTo)
            .HasMaxLength(2_000)
            .IsRequired();

        builder.Property(r => r.CreatedBy)
            .HasMaxLength(36)
            .IsRequired();

        builder.Property<byte[]>("RowVersion")
            .IsRowVersion();
    }
}