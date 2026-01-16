using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using YogaEveryDamnDay.Models;

namespace YogaEveryDamnDay.Migrations
{
    [DbContext(typeof(YogaEveryDamnDayContext))]
    [Migration("20160616171649_Add-Image")]
    partial class AddImage
    {
        protected override void BuildTargetModel(ModelBuilder modelBuilder)
        {
            modelBuilder
                .HasAnnotation("ProductVersion", "1.0.0-rc2-20901")
                .HasAnnotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn);

            modelBuilder.Entity("YogaEveryDamnDay.Models.Category", b =>
                {
                    b.Property<int>("CategoryId")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("PoseType");

                    b.HasKey("CategoryId");

                    b.ToTable("Category");
                });

            modelBuilder.Entity("YogaEveryDamnDay.Models.Pose", b =>
                {
                    b.Property<int>("PoseId")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("CommonName");

                    b.Property<string>("Description");

                    b.Property<string>("Image");

                    b.Property<string>("Sanskrit");

                    b.HasKey("PoseId");

                    b.ToTable("Pose");
                });

            modelBuilder.Entity("YogaEveryDamnDay.Models.PoseCategory", b =>
                {
                    b.Property<int>("PoseCategoryId")
                        .ValueGeneratedOnAdd();

                    b.Property<int>("CategoryId");

                    b.Property<int>("PoseId");

                    b.HasKey("PoseCategoryId");

                    b.HasIndex("CategoryId");

                    b.HasIndex("PoseId");

                    b.ToTable("PoseCategory");
                });

            modelBuilder.Entity("YogaEveryDamnDay.Models.PoseRelationship", b =>
                {
                    b.Property<int>("PoseRelationshipId")
                        .ValueGeneratedOnAdd();

                    b.Property<int?>("BasePosePoseId");

                    b.Property<int?>("PrepPosePoseId");

                    b.HasKey("PoseRelationshipId");

                    b.HasIndex("BasePosePoseId");

                    b.HasIndex("PrepPosePoseId");

                    b.ToTable("PoseRelationship");
                });

            modelBuilder.Entity("YogaEveryDamnDay.Models.PoseCategory", b =>
                {
                    b.HasOne("YogaEveryDamnDay.Models.Category")
                        .WithMany()
                        .HasForeignKey("CategoryId")
                        .OnDelete(DeleteBehavior.Cascade);

                    b.HasOne("YogaEveryDamnDay.Models.Pose")
                        .WithMany()
                        .HasForeignKey("PoseId")
                        .OnDelete(DeleteBehavior.Cascade);
                });

            modelBuilder.Entity("YogaEveryDamnDay.Models.PoseRelationship", b =>
                {
                    b.HasOne("YogaEveryDamnDay.Models.Pose")
                        .WithMany()
                        .HasForeignKey("BasePosePoseId");

                    b.HasOne("YogaEveryDamnDay.Models.Pose")
                        .WithMany()
                        .HasForeignKey("PrepPosePoseId");
                });
        }
    }
}