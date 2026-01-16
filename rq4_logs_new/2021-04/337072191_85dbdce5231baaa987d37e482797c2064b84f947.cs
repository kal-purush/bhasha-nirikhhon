using AutoMapper;
using DAL.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DAL.Data.Configurations
{
    public class RecommendationConfiguration : Profile, IEntityTypeConfiguration<Recommendation>
    {
        public RecommendationConfiguration()
        { }

        public void Configure(EntityTypeBuilder<Recommendation> builder)
        {
            builder.ToTable("tbl_recommendations");
            builder.Property(i => i.Id).HasColumnName("id").ValueGeneratedOnAdd();
            builder.Property(i => i.DysfunctionId).HasColumnName("dysfunction_id").IsRequired();
            builder.Property(i => i.Link).HasColumnName("link").HasColumnType("varchar(500)").HasDefaultValue(null);

            builder.HasKey(i => i.Id).IsClustered();
            builder.HasOne(i => i.Dysfunction).WithMany().HasForeignKey(f => f.DysfunctionId).IsRequired();

            builder.HasData(
                new Recommendation { Id = 1, DysfunctionId = 1, Link = "https://www.tablegroup.com/download/personal-histories-exercise/" },
                new Recommendation { Id = 2, DysfunctionId = 1 },
                new Recommendation { Id = 3, DysfunctionId = 1, Link = "https://www.tablegroup.com/hub/post/the-fundamental-attribution-error" },
                new Recommendation { Id = 4, DysfunctionId = 1, Link = "https://www.tablegroup.com/download/itp-self-ranking-exercise/" },
                new Recommendation { Id = 5, DysfunctionId = 1 },

                new Recommendation { Id = 6, DysfunctionId = 2, Link = "https://www.tablegroup.com/hub/post/real-time-permission-video" },
                new Recommendation { Id = 7, DysfunctionId = 2 },
                new Recommendation { Id = 8, DysfunctionId = 2 },
                new Recommendation { Id = 9, DysfunctionId = 2 },
                new Recommendation { Id = 10, DysfunctionId = 2, Link = "https://www.tablegroup.com/hub/post/conflict-continuum" },

                new Recommendation { Id = 11, DysfunctionId = 3 },
                new Recommendation { Id = 12, DysfunctionId = 3, Link = "https://www.tablegroup.com/download/silos-model/" },
                new Recommendation { Id = 13, DysfunctionId = 3 },
                new Recommendation { Id = 14, DysfunctionId = 3 },
                new Recommendation { Id = 15, DysfunctionId = 3 },

                new Recommendation { Id = 16, DysfunctionId = 4, Link = "https://www.tablegroup.com/download/team-effectiveness-exercise/" },
                new Recommendation { Id = 17, DysfunctionId = 4 },
                new Recommendation { Id = 18, DysfunctionId = 4 },

                new Recommendation { Id = 19, DysfunctionId = 5, Link = "https://www.tablegroup.com/hub/post/team-1" },
                new Recommendation { Id = 20, DysfunctionId = 5 },
                new Recommendation { Id = 21, DysfunctionId = 5 }
                );
        }
    }
}