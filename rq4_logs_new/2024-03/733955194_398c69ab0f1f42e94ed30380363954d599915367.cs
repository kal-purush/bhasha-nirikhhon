using KlockanAPI.Domain.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KlockanAPI.Infrastructure.Data.Seeders
{
    internal static class ClassroomSeeder
    {
        public static ModelBuilder SeedClassrooms(this ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Classroom>().HasData(
                new Classroom
                {
                    Id = 1,
                    CourseId = 1,
                    ProgramId = 1,
                    StartDate = new DateOnly(2024, 2, 23),
                    Meetings = [],
                    Schedule = [],
                    ClassroomUsers = [],
                    CreatedAt = new DateTime(2024, 1, 23, 0, 0, 0, DateTimeKind.Utc)
                },
                new Classroom
                {
                    Id = 2,
                    CourseId = 2,
                    ProgramId = 1,
                    StartDate = new DateOnly(2024, 2, 23),
                    Meetings = [],
                    Schedule = [],
                    ClassroomUsers = [],
                    CreatedAt = new DateTime(2024, 1, 23, 0, 0, 0, DateTimeKind.Utc)
                },
                new Classroom
                {
                    Id = 3,
                    CourseId = 1,
                    ProgramId = 2,
                    StartDate = new DateOnly(2024, 2, 23),
                    Meetings = [],
                    Schedule = [],
                    ClassroomUsers = [],
                    CreatedAt = new DateTime(2024, 1, 23, 0, 0, 0, DateTimeKind.Utc)
                }
            );

            return modelBuilder;
        }
    }
}