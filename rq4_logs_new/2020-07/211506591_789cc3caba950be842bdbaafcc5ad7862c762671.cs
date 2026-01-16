using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace Data.Configurations
{
    public class CharacterListRelationConfiguration : IEntityTypeConfiguration<CharacterListRelation>
    {
        public void Configure(EntityTypeBuilder<CharacterListRelation> builder)
        {
            builder.ToTable(nameof(CharacterListRelation));

            builder.HasKey(charListRelation => new { charListRelation.CharacterListId, charListRelation.TibiaCharacterId });
            
            builder
                .HasOne(charListRelation => charListRelation.CharacterList)
                .WithMany(charList => charList.CharacterListRelations)
                .HasForeignKey(clr => clr.CharacterListId);
            
            builder
                .HasOne(charListRelation => charListRelation.TibiaCharacter)
                .WithMany(tibiaChar => tibiaChar.CharacterListRelations)
                .HasForeignKey(clr => clr.TibiaCharacterId);
        }
    }
}