using BookStore.EF6.Entities;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Entity.Infrastructure.Annotations;
using System.Data.Entity.ModelConfiguration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookStore.EF6.Configurations
{
    class EAuthorConfiguration : EntityTypeConfiguration<EAuthor>
    {
        public EAuthorConfiguration()
        {
            ToTable("Authors", "BookStore");

            Property(a => a.Name)
                .HasMaxLength(200)
                .IsRequired()
                .HasColumnAnnotation(
                    IndexAnnotation.AnnotationName,
                    new IndexAnnotation(new IndexAttribute { IsUnique = true })
                );

            HasMany(a => a.Books)
                .WithMany(e => e.Authors);
        }
    }
}