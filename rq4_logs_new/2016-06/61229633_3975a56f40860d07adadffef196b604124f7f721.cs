using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Capstone.Models;

namespace Capstone.Migrations
{
    [DbContext(typeof(CapstoneContext))]
    [Migration("20160619204321_AddedProcessorType")]
    partial class AddedProcessorType
    {
        protected override void BuildTargetModel(ModelBuilder modelBuilder)
        {
            modelBuilder
                .HasAnnotation("ProductVersion", "1.0.0-rc2-20901")
                .HasAnnotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn);

            modelBuilder.Entity("Capstone.Models.Images", b =>
                {
                    b.Property<int>("ImageId")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("ImageUrl");

                    b.HasKey("ImageId");

                    b.ToTable("Images");
                });

            modelBuilder.Entity("Capstone.Models.Phones", b =>
                {
                    b.Property<int>("PhoneId")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("AndroidVersion");

                    b.Property<int>("BatteryMah");

                    b.Property<string>("Camera");

                    b.Property<string>("Carrier");

                    b.Property<string>("DisplayResolution");

                    b.Property<bool>("ExpandableStorage");

                    b.Property<string>("GPU");

                    b.Property<int>("ImageId");

                    b.Property<bool>("MicroUsb");

                    b.Property<string>("Name");

                    b.Property<decimal>("Price");

                    b.Property<string>("Processor");

                    b.Property<bool>("QuickCharging");

                    b.Property<int>("RamSize");

                    b.Property<DateTime>("ReleaseDate");

                    b.Property<decimal>("ScreenSize");

                    b.Property<bool>("USBC");

                    b.Property<bool>("WaterProof");

                    b.Property<bool>("WirelessCharging");

                    b.Property<int>("processorType");

                    b.HasKey("PhoneId");

                    b.ToTable("Phones");
                });

            modelBuilder.Entity("Capstone.Models.Users", b =>
                {
                    b.Property<int>("UserId")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("Email");

                    b.Property<string>("Name");

                    b.HasKey("UserId");

                    b.ToTable("Users");
                });
        }
    }
}