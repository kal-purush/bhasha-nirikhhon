using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Workflow_Models;
using Workflow_Models.Models;

namespace Workflow.Migrations
{
    [DbContext(typeof(DatabaseConfiguration))]
    [Migration("20161231135407_Second")]
    partial class Second
    {
        protected override void BuildTargetModel(ModelBuilder modelBuilder)
        {
            modelBuilder
                .HasAnnotation("ProductVersion", "1.1.0-rtm-22752")
                .HasAnnotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn);

            modelBuilder.Entity("Workflow_Models.Models.Document", b =>
                {
                    b.Property<int>("ID")
                        .ValueGeneratedOnAdd();

                    b.Property<int?>("MetaDataID");

                    b.Property<int>("MyProperty");

                    b.Property<int?>("StatusID");

                    b.HasKey("ID");

                    b.HasIndex("MetaDataID");

                    b.HasIndex("StatusID");

                    b.ToTable("Document");
                });

            modelBuilder.Entity("Workflow_Models.Models.Keyword", b =>
                {
                    b.Property<int>("ID")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("Keywords");

                    b.Property<int?>("MetaDataID");

                    b.HasKey("ID");

                    b.HasIndex("MetaDataID");

                    b.ToTable("Keyword");
                });

            modelBuilder.Entity("Workflow_Models.Models.MetaData", b =>
                {
                    b.Property<int>("ID")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("Abstract");

                    b.Property<string>("Created");

                    b.Property<int>("PersonId");

                    b.Property<double>("Version");

                    b.HasKey("ID");

                    b.ToTable("Metadata");
                });

            modelBuilder.Entity("Workflow_Models.Models.Status", b =>
                {
                    b.Property<int>("ID")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("Stat");

                    b.Property<double>("VersionType");

                    b.HasKey("ID");

                    b.ToTable("Status");
                });

            modelBuilder.Entity("Workflow_Models.Models.User", b =>
                {
                    b.Property<int>("ID")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("Email")
                        .IsRequired();

                    b.Property<string>("FirstName")
                        .IsRequired();

                    b.Property<string>("LastName")
                        .IsRequired();

                    b.Property<string>("Password")
                        .IsRequired();

                    b.Property<int>("Permission");

                    b.HasKey("ID");

                    b.ToTable("User");
                });

            modelBuilder.Entity("Workflow_Models.Models.UserLog", b =>
                {
                    b.Property<int>("ID")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("Date")
                        .IsRequired();

                    b.Property<int>("UserId");

                    b.HasKey("ID");

                    b.HasIndex("UserId");

                    b.ToTable("UserLog");
                });

            modelBuilder.Entity("Workflow_Models.Models.Document", b =>
                {
                    b.HasOne("Workflow_Models.Models.MetaData", "MetaData")
                        .WithMany()
                        .HasForeignKey("MetaDataID");

                    b.HasOne("Workflow_Models.Models.Status", "Status")
                        .WithMany()
                        .HasForeignKey("StatusID");
                });

            modelBuilder.Entity("Workflow_Models.Models.Keyword", b =>
                {
                    b.HasOne("Workflow_Models.Models.MetaData")
                        .WithMany("Keywords")
                        .HasForeignKey("MetaDataID");
                });

            modelBuilder.Entity("Workflow_Models.Models.UserLog", b =>
                {
                    b.HasOne("Workflow_Models.Models.User", "User")
                        .WithMany("Logs")
                        .HasForeignKey("UserId")
                        .OnDelete(DeleteBehavior.Cascade);
                });
        }
    }
}