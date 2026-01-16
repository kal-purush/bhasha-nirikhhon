using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore;
using AnimalShelter.DataAccess.Entities;
using AnimalShelter.Core.Models;

namespace AnimalShelter.DataAccess.Configurations
{
    public class AdoptionApplicationConfiguration : IEntityTypeConfiguration<AdoptionApplicationEntity>
    {
        public void Configure(EntityTypeBuilder<AdoptionApplicationEntity> builder)
        {
            builder.HasKey(x => x.Id);

            builder.Property(b => b.ApplicationDate)
                .IsRequired();

            builder.Property(b => b.Comment)
                .HasMaxLength(AdoptionApplication.MAX_COMMENT_LENGTH)
                .IsRequired();

            // Конфигурация внешних ключей
            builder.HasOne(a => a.User)
                .WithMany(t => t.AdoptionApplication)
                .HasForeignKey(a => a.UserId)
                .OnDelete(DeleteBehavior.Restrict)
                .IsRequired();

            builder.HasOne(a => a.Animal)
                .WithMany(s => s.AdoptionApplication)
                .HasForeignKey(a => a.AnimalId)
                .OnDelete(DeleteBehavior.Restrict)
                .IsRequired();

            builder.HasOne(a => a.StatusAdoption)
                .WithMany(h => h.AdoptionApplication)
                .HasForeignKey(a => a.StatusAdoptionId)
                .OnDelete(DeleteBehavior.Restrict)
                .IsRequired();

            // Индексы для внешних ключей
            builder.HasIndex(a => a.UserId);
            builder.HasIndex(a => a.AnimalId);
            builder.HasIndex(a => a.StatusAdoptionId);
        }
    }
}