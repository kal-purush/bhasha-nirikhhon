using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using SFA.DAS.Recruit.Api.Domain.Entities;
using SFA.DAS.Recruit.Api.Domain.Models;

namespace SFA.DAS.Recruit.Api.Data.VacancyReview;

[ExcludeFromCodeCoverage]
public class VacancyReviewEntityConfiguration: IEntityTypeConfiguration<VacancyReviewEntity>
{
    public void Configure(EntityTypeBuilder<VacancyReviewEntity> builder)
    {
        builder.ToTable("VacancyReview");
        builder.HasKey(x => x.Id);
        builder.Property(x => x.VacancyReference).HasConversion(x => x.Value, x => new VacancyReference(x));
        builder.Property(x => x.Status).HasConversion(v => v.ToString(), v => (ReviewStatus)Enum.Parse(typeof(ReviewStatus), v));
    }
}