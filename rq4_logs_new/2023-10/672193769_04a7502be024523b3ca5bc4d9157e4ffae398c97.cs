using api.SpotSolar.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace api.Data.ModelsConfigurations;

public class ProposalConfiguration : IEntityTypeConfiguration<Proposal>
{
    public void Configure(EntityTypeBuilder<Proposal> builder)
    {

        builder.OwnsOne(x => x.Customer, c =>
        {
            c.Property(x => x.Email).HasColumnType("varchar").HasMaxLength(255);
            c.Property(x => x.Name).HasColumnType("varchar").HasMaxLength(255);
            c.Property(x => x.Phone).HasColumnType("varchar").HasMaxLength(255);
        });

        builder.OwnsOne(x => x.Address, a =>
        {
            a.Property(x => x.Street).HasColumnType("varchar").HasMaxLength(255);
            a.Property(x => x.Number).HasColumnType("varchar").HasMaxLength(255);
            a.Property(x => x.City).HasColumnType("varchar").HasMaxLength(255);
            a.Property(x => x.State).HasColumnType("varchar").HasMaxLength(255);
            a.Property(x => x.ZipCode).HasColumnType("varchar").HasMaxLength(255);
        });

        builder.OwnsMany(x => x.Products, p =>
        {
            p.Property(x => x.Name).HasColumnType("varchar").HasMaxLength(255);
        });

        builder.Property(x => x.Notes).HasColumnType("varchar").HasMaxLength(255);
        builder.Property(x => x.Power).HasColumnType("varchar").HasMaxLength(255);
        builder.Property(x => x.TotalPrice).HasPrecision(18, 8);
        builder.Property(x => x.TotalPriceProducts).HasPrecision(18, 8);
        builder.Property(x => x.LabourValue).HasPrecision(18, 8);
    }
}