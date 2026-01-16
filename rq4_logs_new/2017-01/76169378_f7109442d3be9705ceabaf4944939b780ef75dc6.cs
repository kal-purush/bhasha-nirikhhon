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
    [Migration("20170120220204_fckstring")]
    partial class fckstring
    {
        protected override void BuildTargetModel(ModelBuilder modelBuilder)
        {
            modelBuilder
                .HasAnnotation("ProductVersion", "1.1.0-rtm-22752")
                .HasAnnotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn);

            modelBuilder.Entity("Workflow_Models.Models.Date", b =>
                {
                    b.Property<int>("ID")
                        .ValueGeneratedOnAdd();

                    b.Property<int>("Day");

                    b.Property<int>("Hour");

                    b.Property<int>("Minute");

                    b.Property<int>("Month");

                    b.Property<int>("Second");

                    b.Property<int>("Year");

                    b.HasKey("ID");

                    b.ToTable("Date");
                });

            modelBuilder.Entity("Workflow_Models.Models.Department", b =>
                {
                    b.Property<int>("ID")
                        .ValueGeneratedOnAdd();

                    b.Property<int?>("FluxID");

                    b.Property<string>("Name");

                    b.Property<int?>("UserID");

                    b.HasKey("ID");

                    b.HasIndex("FluxID");

                    b.HasIndex("UserID");

                    b.ToTable("Department");
                });

            modelBuilder.Entity("Workflow_Models.Models.Document", b =>
                {
                    b.Property<int>("ID")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("FileExtension");

                    b.Property<string>("FileName");

                    b.Property<int?>("FluxID");

                    b.Property<int?>("MetaDataID");

                    b.Property<string>("Path");

                    b.Property<int?>("StatusID");

                    b.HasKey("ID");

                    b.HasIndex("FluxID");

                    b.HasIndex("MetaDataID");

                    b.HasIndex("StatusID");

                    b.ToTable("Document");
                });

            modelBuilder.Entity("Workflow_Models.Models.Flux", b =>
                {
                    b.Property<int>("ID")
                        .ValueGeneratedOnAdd();

                    b.HasKey("ID");

                    b.ToTable("Flux");
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

                    b.Property<int?>("CreatedID");

                    b.Property<string>("UserEmail");

                    b.HasKey("ID");

                    b.HasIndex("CreatedID");

                    b.ToTable("Metadata");
                });

            modelBuilder.Entity("Workflow_Models.Models.Status", b =>
                {
                    b.Property<int>("ID")
                        .ValueGeneratedOnAdd();

                    b.Property<int>("Stat");

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

                    b.Property<string>("FirstName");

                    b.Property<string>("LastName");

                    b.Property<string>("Password")
                        .IsRequired()
                        .HasMaxLength(100);

                    b.Property<int>("Permission");

                    b.HasKey("ID");

                    b.ToTable("User");
                });

            modelBuilder.Entity("Workflow_Models.Models.UserLog", b =>
                {
                    b.Property<int>("ID")
                        .ValueGeneratedOnAdd();

                    b.Property<int>("DateID");

                    b.Property<int>("UserId");

                    b.Property<int>("UserType");

                    b.HasKey("ID");

                    b.HasIndex("DateID");

                    b.HasIndex("UserId");

                    b.ToTable("UserLog");
                });

            modelBuilder.Entity("Workflow_Models.Models.Department", b =>
                {
                    b.HasOne("Workflow_Models.Models.Flux")
                        .WithMany("Departments")
                        .HasForeignKey("FluxID");

                    b.HasOne("Workflow_Models.Models.User", "User")
                        .WithMany()
                        .HasForeignKey("UserID");
                });

            modelBuilder.Entity("Workflow_Models.Models.Document", b =>
                {
                    b.HasOne("Workflow_Models.Models.Flux")
                        .WithMany("Documents")
                        .HasForeignKey("FluxID");

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

            modelBuilder.Entity("Workflow_Models.Models.MetaData", b =>
                {
                    b.HasOne("Workflow_Models.Models.Date", "Created")
                        .WithMany()
                        .HasForeignKey("CreatedID");
                });

            modelBuilder.Entity("Workflow_Models.Models.UserLog", b =>
                {
                    b.HasOne("Workflow_Models.Models.Date", "Date")
                        .WithMany()
                        .HasForeignKey("DateID")
                        .OnDelete(DeleteBehavior.Cascade);

                    b.HasOne("Workflow_Models.Models.User", "User")
                        .WithMany("Logs")
                        .HasForeignKey("UserId")
                        .OnDelete(DeleteBehavior.Cascade);
                });
        }
    }
}