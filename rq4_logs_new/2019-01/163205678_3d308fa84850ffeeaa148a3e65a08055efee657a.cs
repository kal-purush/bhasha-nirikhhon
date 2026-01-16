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
    public class StudyConfiguration : IEntityTypeConfiguration<Study>
    {
        public void Configure(EntityTypeBuilder<Study> entity)
        {
            entity.HasKey(e => new { e.Id });

            entity.Property(e => e.Id)
                .ValueGeneratedOnAdd();

            entity.HasIndex(e => e.Name);

            entity.HasMany(c => c.StudyTopics)
                .WithOne(u => u.Study)
                .HasForeignKey(u => u.StudyId);
        }
    }
}