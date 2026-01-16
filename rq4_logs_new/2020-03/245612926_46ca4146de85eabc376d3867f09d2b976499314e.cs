using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using CourseGenerator.Models.Entities.Info;
using CourseGenerator.Models.Entities.CourseAccess;

namespace CourseGenerator.Models.Configs.CourseAccess
{
    class UserCourseConfig: IEntityTypeConfiguration<UserCourse>
    {
        public void Configure(EntityTypeBuilder<UserCourse> builder)
        {
            builder.HasKey(p => new { p.UserId, p.CourseId });
            builder.Property(p => p.Note).IsUnicode();

            builder.HasOne(p => p.User)
                .WithMany(p => p.UserCourses)
                .HasForeignKey(p =>p.UserId);

            builder.HasOne(p => p.Course)
                .WithMany(p => p.UserCourses)
                .HasForeignKey(p => p.CourseId);

            builder.HasOne(p => p.Level)
                .WithMany(p => p.UserCourses)
                .HasForeignKey(p => p.LevelId);

        }
    }
}