using api.Models.Cryptos;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace api.Data.ModelsConfigurations;

public class CryptoAssetConfiguration : IEntityTypeConfiguration<CryptoAsset>
{
    public void Configure(EntityTypeBuilder<CryptoAsset> builder)
    {
        builder.Property(x => x.Symbol).HasColumnType("varchar").HasMaxLength(255);
        builder.Property(x => x.CurrencyName).HasColumnType("varchar").HasMaxLength(255);
        builder.Property(x => x.CryptoCurrency).HasColumnType("varchar").HasMaxLength(255);
        builder.Property(x => x.Balance).HasPrecision(18, 8);
        builder.Property(x => x.AveragePrice).HasPrecision(18, 8);
        builder.HasMany(x => x.Addresses).WithOne(x => x.CryptoAsset);
    }
}