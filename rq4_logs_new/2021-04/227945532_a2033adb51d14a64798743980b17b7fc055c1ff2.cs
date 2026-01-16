using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Mithrill.MonsterBook.Domain;

namespace Mithrill.MonsterBook.Infrastructure.Configurations
{
    internal sealed class CreatureFlawConfiguration : IEntityTypeConfiguration<CreatureFlaw>
    {
        public void Configure(EntityTypeBuilder<CreatureFlaw> builder)
        {
            builder.HasKey(creatureFlaw => new { creatureFlaw.CreatureId, creatureFlaw.FlawId });

            builder.HasOne(creatureFlaw => creatureFlaw.Creature)
                .WithMany(creature => creature.CreatureFlaws)
                .HasForeignKey(creatureMerit => creatureMerit.CreatureId);

            builder.HasOne(creatureFlaw => creatureFlaw.Flaw)
                .WithMany(merit => merit.CreatureFlaws)
                .HasForeignKey(creatureMerit => creatureMerit.FlawId);

            builder.ToTable("CreatureFlaw");
        }
    }
}