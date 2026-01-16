using api.Models.Cryptos;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace api.Data.ModelsConfigurations;

public class CryptoTransactionConfiguration : IEntityTypeConfiguration<CryptoTransaction>
{
    public void Configure(EntityTypeBuilder<CryptoTransaction> builder)
    {
        builder.Property(x => x.ExchangeName).HasColumnType("varchar").HasMaxLength(255);
        builder.Property(x => x.Amount).HasPrecision(18, 8);
        builder.Property(x => x.Price).HasPrecision(18, 8);
    }
}