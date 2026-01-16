using BaseProject.Data.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BaseProject.Data.Configurations
{
    public class AuthorConfiguration : IEntityTypeConfiguration<Author>
    {
        public void Configure(EntityTypeBuilder<Author> builder)
        {
            builder.ToTable("Author");
            builder.HasKey(x => x.AuthorID);
            builder.Property(x => x.Name).IsRequired().HasMaxLength(30);

            // Relationship: Một tác giả có nhiều bài đăng
            builder.HasMany(a => a.Posts).WithOne(p => p.Author).HasForeignKey(p => p.AuthorID);
        }
    }
}