using Circle.Data.Models;
using Microsoft.EntityFrameworkCore;

namespace Circle.Data.Extensions
{
	public static class ModelBuilderExtensions
	{
		public static void ConfigureMetadataEntity<TEntity>(this ModelBuilder builder) where TEntity : MetadataBaseEntity
		{
			builder.Entity<TEntity>()
				.HasOne(u => u.CreatedBy)
				.WithMany()
				.OnDelete(DeleteBehavior.NoAction);

			builder.Entity<TEntity>()
				.HasOne(u => u.UpdatedBy)
				.WithMany()
				.OnDelete(DeleteBehavior.NoAction);

			builder.Entity<TEntity>()
				.HasOne(u => u.DeletedBy)
				.WithMany()
				.OnDelete(DeleteBehavior.NoAction);
		}
	}
}