using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using AJS.Data.Models;

/// <summary>
///  AdCategoryTranslation Data Model Configuration
/// </summary>
namespace AJS.Data.EntityConfiguration
{
   public class AdCategoryTranslationConfiguration : IEntityTypeConfiguration<AdCategoryTranslation>
    {
        public void Configure(EntityTypeBuilder<AdCategoryTranslation> builder)
        {
            builder.HasKey(k => k.AdCategoryTranslationId);
        }
    }
}