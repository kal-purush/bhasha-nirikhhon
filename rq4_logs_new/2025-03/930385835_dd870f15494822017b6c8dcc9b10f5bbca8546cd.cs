using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using PetFamily.Accounts.Domain.DataModels;
using PetFamily.Core.Dto.Pet;
using PetFamily.Core.Extensions;

namespace PetFamily.Accounts.Infrastructure.Configurations;

public class ParticipantAccountConfiguration : IEntityTypeConfiguration<ParticipantAccount>
{
    public void Configure(EntityTypeBuilder<ParticipantAccount> builder)
    {
        builder.ToTable("participant_accounts");
        
        builder
            .Property(p => p.FavoritePets)
            .CustomListJsonCollectionConverter(
                favoritePet => new FavoritePetDto(favoritePet.Name, favoritePet.PetId),
                dto => new FavoritePetDto(dto.Name, dto.PetId))
            .HasColumnName("favorite_pets");
    }
}