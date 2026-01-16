using Microsoft.EntityFrameworkCore;
using MVC_Ecommerce.Models;

namespace Web_MVC_Commerce.Data
{
    public class AccountMap : IEntityTypeConfiguration<Account>
    {
        public void Configure(Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder<Account> builder)
        {
            builder.ToTable("Accounts");
            builder.HasKey(e => e.AccountId);
            builder.Property(e => e.AccountName).HasMaxLength(50).IsRequired();
            builder.Property(e => e.Balance).HasColumnType("decimal(18,2)").IsRequired();
            /*
                 builder.HasOne<User>()
                    .WithMany()
                    .HasForeignKey(e => e.UserId);
            */
        }
    }
}