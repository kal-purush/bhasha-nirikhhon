using MoviesApi.Models;

namespace MoviesApi.Config
{
	public class MovieConfig : IEntityTypeConfiguration<Movie>
	{
		public void Configure(EntityTypeBuilder<Movie> builder)
		{
			builder.HasKey(M => M.ID);

			builder.Property(M => M.Title).IsRequired().HasMaxLength(250);

			builder.Property(M => M.Year).HasMaxLength(9999);

			builder.Property(M => M.Rate)
				 .HasPrecision(2, 1)
				 .IsRequired();

			builder.Property(M => M.StoryLine).HasMaxLength(2500);

			builder
				.HasOne(M => M.Genre)
				.WithMany(G => G.Movies)
				.HasForeignKey(M => M.GenreID);

			builder
				.HasMany(M => M.CharacterActInMovies)
				.WithOne(CM => CM.Movie);


		}
	}
}