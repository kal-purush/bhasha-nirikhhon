using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MyAPI.Entities.Configurations
{
    public class MovieActorConfiguration : IEntityTypeConfiguration<MovieActor>
    {
        public void Configure(EntityTypeBuilder<MovieActor> builder)
        {
            builder.HasKey(a => new { a.ActorId, a.MovieId });

            builder
                .HasOne(a => a.Movie)
                .WithMany(a => a.MovieActors)
                .HasForeignKey(a => a.MovieId);

            builder
                .HasOne(a => a.Actor)
                .WithMany(a => a.MovieActors)
                .HasForeignKey(a => a.ActorId);
        }
    }
}