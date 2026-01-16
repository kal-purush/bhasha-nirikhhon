using EKlubas.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EKlubas.Persistence.Configurations
{
    public class RewardHistoryConfiguration : IEntityTypeConfiguration<RewardHistory>
    {
        public void Configure(EntityTypeBuilder<RewardHistory> entity)
        {
            entity.HasKey(e => new { e.Id });

            entity.Property(e => e.Id)
                .ValueGeneratedOnAdd();

            entity.HasIndex(e => e.UserId);

            entity.Property(e => e.Reward)
                .IsRequired();

            entity.HasOne(rh => rh.User)
                .WithMany(u => u.RewardHistories)
                .HasForeignKey(rh => rh.UserId);
        }
    }
}