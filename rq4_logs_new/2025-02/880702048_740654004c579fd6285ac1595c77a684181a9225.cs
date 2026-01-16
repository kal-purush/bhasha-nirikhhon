using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using P2Project.VolunteerRequests.Domain;
using P2Project.VolunteerRequests.Domain.ValueObjects;

namespace P2Project.VolunteerRequests.Infrastructure.Configurations.Write;

public class VolunteerRequestConfiguration :
    IEntityTypeConfiguration<VolunteerRequest>
{
    public void Configure(EntityTypeBuilder<VolunteerRequest> builder)
    {
        builder.ToTable("volunteer_requests");
        
        builder.HasKey(b => b.RequestId);
        
        builder.Property(s => s.RequestId)
            .IsRequired()
            .HasColumnName("request_Id");
        
        builder.Property(s => s.AdminId)
            .IsRequired(false)
            .HasColumnName("admin_Id");
        
        builder.Property(s => s.UserId)
            .HasColumnName("user_Id");
        
        builder.ComplexProperty(v => v.FullName, fnb =>
        {
            fnb.Property(fn => fn.FirstName)
                .IsRequired()
                .HasColumnName("first_name");
            
            fnb.Property(fn => fn.SecondName)
                .IsRequired()
                .HasColumnName("second_name");
            
            fnb.Property(fn => fn.LastName)
                .IsRequired()
                .HasColumnName("last_name");
        });
        
        builder.ComplexProperty(v => v.VolunteerInfo, vib =>
        {
            vib.Property(vi => vi.Age)
                .IsRequired()
                .HasColumnName("age");

            vib.Property(vi => vi.Grade)
                .IsRequired()
                .HasColumnName("grade");
        });
        
        builder.Property(s => s.DiscussionId)
            .IsRequired(false)
            .HasColumnName("discussion_id");
        
        builder.Property(v => v.Status)
            .HasConversion<string>()
            .HasColumnName("status")
            .IsRequired();
        
        builder.Property(v => v.CreatedAt)
            .IsRequired()
            .HasColumnName("created_at");
        
        builder.Property(v => v.RejectionComment)
            .HasConversion(
                i => i.Value,
                value => RejectionComment.Create(value).Value)
            .IsRequired(false)
            .HasColumnName("rejection_comment");
    }
}