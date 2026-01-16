using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Stock.Core.Models.Stock;

namespace Stock.Infrastructure.Persistence.Configurations
{
    public class StockWithPriceTagConfiguration : IEntityTypeConfiguration<StockWithPriceTag>
    {
        public void Configure(EntityTypeBuilder<StockWithPriceTag> builder)
        {
            builder.ToSqlQuery($"""
                                SELECT s.id as stockid,
                                    s.symbol as symbol,
                                    CASE 
                                        WHEN spo.price is NULL THEN -1
                                    ELSE spo.price
                                    END as price,
                                    s.isactive as isactive,
                                    spo.createdat as lastpriceupdate,
                                    s.createdat as createdat
                                    FROM public.stocks AS s
                                    LEFT JOIN LATERAL
                                (SELECT
                                    sp.price,
                                    sp.createdat
                                    FROM public.stocks_prices AS sp
                                    WHERE sp.stockid = s.id
                                    ORDER BY sp.createdat DESC
                                    LIMIT 1) AS spo ON true
                                """);
        }
    }
}