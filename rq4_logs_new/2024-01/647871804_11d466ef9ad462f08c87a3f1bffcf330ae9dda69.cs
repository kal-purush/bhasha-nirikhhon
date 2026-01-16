using MeowLib.Domain.Book.Entity;
using MeowLib.Domain.BookComment.Entity;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace MeowLib.DAL.Configuration;

public class BookEntityModelConfiguration : IEntityTypeConfiguration<BookEntityModel>
{
    public void Configure(EntityTypeBuilder<BookEntityModel> builder)
    {
        builder
            .HasMany(b => b.Tags)
            .WithMany(t => t.Books);

        builder
            .HasMany(b => b.Translations)
            .WithOne(t => t.Book)
            .OnDelete(DeleteBehavior.Cascade);

        builder
            .HasMany(b => b.Peoples)
            .WithOne(b => b.Book)
            .OnDelete(DeleteBehavior.Cascade);

        builder
            .HasMany<BookCommentEntityModel>()
            .WithOne(c => c.Book)
            .OnDelete(DeleteBehavior.Cascade);
    }
}