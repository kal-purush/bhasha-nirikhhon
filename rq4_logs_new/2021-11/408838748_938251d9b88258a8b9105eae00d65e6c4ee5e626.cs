using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using SkillMatrix.Domain.Users.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SkillMatrix.DataAccess.Configurations
{
    public class SkillRatingConfiguration : IEntityTypeConfiguration<SkillRating>
    {
         public void Configure(EntityTypeBuilder<SkillRating> builder)
         {
            builder.HasKey(u => new { u.SkillName, u.UserId });
         }
    }
}