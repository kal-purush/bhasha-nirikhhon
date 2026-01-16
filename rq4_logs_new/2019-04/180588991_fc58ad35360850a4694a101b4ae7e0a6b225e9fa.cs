using Catalog.Domain.Entity;
using Catalog.Domain.Enum;
using Catalog.Domain.Helpers;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Catalog.DataAccessLayer.Configuration
{
    public class MessageConfiguration : IEntityTypeConfiguration<Message>
    {
        public void Configure(EntityTypeBuilder<Message> builder)
        {
            builder.ConfigureBaseEntityProperties();

            builder.HasIndex(entity => entity.MessageId);
            builder.HasIndex(entity => entity.MessageDigest).IsUnique();

            builder.Property(entity => entity.MessageId).IsRequired().HasMaxLength(500);
            builder.Property(entity => entity.MessageDigest).IsRequired().HasMaxLength(500);

            builder.Property(entity => entity.MessageProtocolVersion).IsRequired().HasMaxLength(10);
            builder.Property(entity => entity.MessageIssue).IsRequired(false).HasMaxLength(500);
            builder.Property(entity => entity.MessageUserId).IsRequired(false).HasMaxLength(500);
            builder.Property(entity => entity.MessageState).IsRequired().HasConversion(state => state.Name,
                dbValue => MessageState.FromName(dbValue, true));


            builder.Property(entity => entity.ConsumerInstance).IsRequired().HasMaxLength(20);
            builder.Property(entity => entity.ConsumerMemberClass).IsRequired().HasMaxLength(20);
            builder.Property(entity => entity.ConsumerMemberCode).IsRequired().HasMaxLength(100);
            builder.Property(entity => entity.ConsumerSubSystemCode).IsRequired().HasMaxLength(100);

            builder.Property(entity => entity.ProducerInstance).IsRequired().HasMaxLength(20);
            builder.Property(entity => entity.ProducerMemberClass).IsRequired().HasMaxLength(20);
            builder.Property(entity => entity.ProducerMemberCode).IsRequired().HasMaxLength(100);
            builder.Property(entity => entity.ProducerSubSystemCode).IsRequired(false).HasMaxLength(100);
            builder.Property(entity => entity.ProducerServiceCode).IsRequired().HasMaxLength(100);
            builder.Property(entity => entity.ProducerServiceVersion).IsRequired(false).HasMaxLength(20);

            builder.Property(entity => entity.ConsumerSecurityServerAddress).IsRequired(false).HasMaxLength(200);
            builder.Property(entity => entity.ConsumerSecurityServerInternalIpAddress).IsRequired(false)
                .HasMaxLength(200);
            builder.Property(entity => entity.ProducerSecurityServerAddress).IsRequired(false).HasMaxLength(200);
            builder.Property(entity => entity.ProducerSecurityServerInternalIpAddress).IsRequired(false)
                .HasMaxLength(200);

            builder.OwnsOne(entity => entity.ConsumerMessageLifecycle, lifecycle =>
            {
                lifecycle.Property(o => o.RequestInTs).IsRequired(false).HasColumnName("ConsumerRequestInTs");
                lifecycle.Property(o => o.RequestOutTs).IsRequired(false).HasColumnName("ConsumerRequestOutTs");
                lifecycle.Property(o => o.ResponseInTs).IsRequired(false).HasColumnName("ConsumerResponseInTs");
                lifecycle.Property(o => o.ResponseOutTs).IsRequired(false).HasColumnName("ConsumerResponseOutTs`");
            });

            builder.OwnsOne(entity => entity.ProducerMessageLifecycle, lifecycle =>
            {
                lifecycle.Property(o => o.RequestInTs).IsRequired(false).HasColumnName("ProducerRequestInTs");
                lifecycle.Property(o => o.RequestOutTs).IsRequired(false).HasColumnName("ProducerRequestOutTs");
                lifecycle.Property(o => o.ResponseInTs).IsRequired(false).HasColumnName("ProducerResponseInTs");
                lifecycle.Property(o => o.ResponseOutTs).IsRequired(false).HasColumnName("ProducerResponseOutTs`");
            });

            builder.Property(entity => entity.RequestAttachmentsCount).IsRequired(false);
            builder.Property(entity => entity.RequestSoapSize).IsRequired(false);
            builder.Property(entity => entity.ResponseAttachmentsCount).IsRequired(false);
            builder.Property(entity => entity.ResponseSoapSize).IsRequired(false);
            builder.Property(entity => entity.IsSucceeded).IsRequired();
            builder.Property(entity => entity.FaultCode).IsRequired(false).HasMaxLength(500);
            builder.Property(entity => entity.FaultString).IsRequired(false).HasMaxLength(1000);
        }
    }
}