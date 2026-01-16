using api.Cryptos.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace api.Data.ModelsConfigurations;

public class CryptoConfiguration : IEntityTypeConfiguration<Crypto>
{
    public void Configure(EntityTypeBuilder<Crypto> builder)
    {
        builder.Property(x => x.Symbol).HasColumnType("varchar").HasMaxLength(255);
        builder.Property(x => x.Name).HasColumnType("varchar").HasMaxLength(255);
        builder.Property(x => x.Image).HasColumnType("varchar").HasMaxLength(1000);
    }
}