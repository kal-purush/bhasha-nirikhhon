using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using KWFCI.Repositories;

namespace KWFCI.Migrations
{
    [DbContext(typeof(ApplicationDbContext))]
    [Migration("20170512055955_AddedStaffInfoToTask")]
    partial class AddedStaffInfoToTask
    {
        protected override void BuildTargetModel(ModelBuilder modelBuilder)
        {
            modelBuilder
                .HasAnnotation("ProductVersion", "1.0.0-rtm-21431")
                .HasAnnotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn);

            modelBuilder.Entity("KWFCI.Models.Broker", b =>
                {
                    b.Property<int>("BrokerID")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("Email");

                    b.Property<bool>("EmailNotifications");

                    b.Property<string>("FirstName");

                    b.Property<string>("LastName");

                    b.Property<string>("Status");

                    b.Property<string>("Type");

                    b.HasKey("BrokerID");

                    b.ToTable("Brokers");
                });

            modelBuilder.Entity("KWFCI.Models.Interaction", b =>
                {
                    b.Property<int>("InteractionID")
                        .ValueGeneratedOnAdd();

                    b.Property<int?>("BrokerID");

                    b.Property<DateTime?>("DateCreated");

                    b.Property<string>("NextStep");

                    b.Property<string>("Notes");

                    b.Property<int?>("StaffProfileID");

                    b.Property<string>("Status");

                    b.Property<int?>("TaskForeignKey");

                    b.HasKey("InteractionID");

                    b.HasIndex("BrokerID");

                    b.HasIndex("StaffProfileID");

                    b.HasIndex("TaskForeignKey")
                        .IsUnique();

                    b.ToTable("Interactions");
                });

            modelBuilder.Entity("KWFCI.Models.KWTask", b =>
                {
                    b.Property<int>("KWTaskID")
                        .ValueGeneratedOnAdd();

                    b.Property<DateTime?>("AlertDate");

                    b.Property<int?>("BrokerID");

                    b.Property<DateTime?>("DateCreated");

                    b.Property<DateTime?>("DateDue");

                    b.Property<bool>("IsComplete");

                    b.Property<string>("Message");

                    b.Property<int>("Priority");

                    b.Property<string>("StaffEmail");

                    b.Property<string>("StaffName");

                    b.Property<int?>("StaffProfileID");

                    b.Property<string>("Type");

                    b.HasKey("KWTaskID");

                    b.HasIndex("BrokerID");

                    b.HasIndex("StaffProfileID");

                    b.ToTable("KWTasks");
                });

            modelBuilder.Entity("KWFCI.Models.Message", b =>
                {
                    b.Property<int>("MessageID")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("Body");

                    b.Property<DateTime>("DateSent");

                    b.Property<int?>("FromStaffProfileID");

                    b.Property<string>("Subject");

                    b.HasKey("MessageID");

                    b.HasIndex("FromStaffProfileID");

                    b.ToTable("Messages");
                });

            modelBuilder.Entity("KWFCI.Models.StaffProfile", b =>
                {
                    b.Property<int>("StaffProfileID")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("Email");

                    b.Property<bool>("EmailNotifications");

                    b.Property<string>("FirstName");

                    b.Property<string>("LastName");

                    b.Property<string>("Role");

                    b.Property<string>("UserId");

                    b.HasKey("StaffProfileID");

                    b.HasIndex("UserId");

                    b.ToTable("StaffProfiles");
                });

            modelBuilder.Entity("KWFCI.Models.StaffUser", b =>
                {
                    b.Property<string>("Id");

                    b.Property<int>("AccessFailedCount");

                    b.Property<string>("ConcurrencyStamp")
                        .IsConcurrencyToken();

                    b.Property<string>("Email")
                        .HasAnnotation("MaxLength", 256);

                    b.Property<bool>("EmailConfirmed");

                    b.Property<bool>("LockoutEnabled");

                    b.Property<DateTimeOffset?>("LockoutEnd");

                    b.Property<string>("NormalizedEmail")
                        .HasAnnotation("MaxLength", 256);

                    b.Property<string>("NormalizedUserName")
                        .HasAnnotation("MaxLength", 256);

                    b.Property<string>("PasswordHash");

                    b.Property<string>("PhoneNumber");

                    b.Property<bool>("PhoneNumberConfirmed");

                    b.Property<string>("SecurityStamp");

                    b.Property<bool>("TwoFactorEnabled");

                    b.Property<string>("UserName")
                        .HasAnnotation("MaxLength", 256);

                    b.HasKey("Id");

                    b.HasIndex("NormalizedEmail")
                        .HasName("EmailIndex");

                    b.HasIndex("NormalizedUserName")
                        .IsUnique()
                        .HasName("UserNameIndex");

                    b.ToTable("AspNetUsers");
                });

            modelBuilder.Entity("Microsoft.AspNetCore.Identity.EntityFrameworkCore.IdentityRole", b =>
                {
                    b.Property<string>("Id");

                    b.Property<string>("ConcurrencyStamp")
                        .IsConcurrencyToken();

                    b.Property<string>("Name")
                        .HasAnnotation("MaxLength", 256);

                    b.Property<string>("NormalizedName")
                        .HasAnnotation("MaxLength", 256);

                    b.HasKey("Id");

                    b.HasIndex("NormalizedName")
                        .HasName("RoleNameIndex");

                    b.ToTable("AspNetRoles");
                });

            modelBuilder.Entity("Microsoft.AspNetCore.Identity.EntityFrameworkCore.IdentityRoleClaim<string>", b =>
                {
                    b.Property<int>("Id")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("ClaimType");

                    b.Property<string>("ClaimValue");

                    b.Property<string>("RoleId")
                        .IsRequired();

                    b.HasKey("Id");

                    b.HasIndex("RoleId");

                    b.ToTable("AspNetRoleClaims");
                });

            modelBuilder.Entity("Microsoft.AspNetCore.Identity.EntityFrameworkCore.IdentityUserClaim<string>", b =>
                {
                    b.Property<int>("Id")
                        .ValueGeneratedOnAdd();

                    b.Property<string>("ClaimType");

                    b.Property<string>("ClaimValue");

                    b.Property<string>("UserId")
                        .IsRequired();

                    b.HasKey("Id");

                    b.HasIndex("UserId");

                    b.ToTable("AspNetUserClaims");
                });

            modelBuilder.Entity("Microsoft.AspNetCore.Identity.EntityFrameworkCore.IdentityUserLogin<string>", b =>
                {
                    b.Property<string>("LoginProvider");

                    b.Property<string>("ProviderKey");

                    b.Property<string>("ProviderDisplayName");

                    b.Property<string>("UserId")
                        .IsRequired();

                    b.HasKey("LoginProvider", "ProviderKey");

                    b.HasIndex("UserId");

                    b.ToTable("AspNetUserLogins");
                });

            modelBuilder.Entity("Microsoft.AspNetCore.Identity.EntityFrameworkCore.IdentityUserRole<string>", b =>
                {
                    b.Property<string>("UserId");

                    b.Property<string>("RoleId");

                    b.HasKey("UserId", "RoleId");

                    b.HasIndex("RoleId");

                    b.HasIndex("UserId");

                    b.ToTable("AspNetUserRoles");
                });

            modelBuilder.Entity("Microsoft.AspNetCore.Identity.EntityFrameworkCore.IdentityUserToken<string>", b =>
                {
                    b.Property<string>("UserId");

                    b.Property<string>("LoginProvider");

                    b.Property<string>("Name");

                    b.Property<string>("Value");

                    b.HasKey("UserId", "LoginProvider", "Name");

                    b.ToTable("AspNetUserTokens");
                });

            modelBuilder.Entity("KWFCI.Models.Interaction", b =>
                {
                    b.HasOne("KWFCI.Models.Broker")
                        .WithMany("Interactions")
                        .HasForeignKey("BrokerID");

                    b.HasOne("KWFCI.Models.StaffProfile")
                        .WithMany("Interactions")
                        .HasForeignKey("StaffProfileID");

                    b.HasOne("KWFCI.Models.KWTask", "Task")
                        .WithOne("Interaction")
                        .HasForeignKey("KWFCI.Models.Interaction", "TaskForeignKey");
                });

            modelBuilder.Entity("KWFCI.Models.KWTask", b =>
                {
                    b.HasOne("KWFCI.Models.Broker")
                        .WithMany("Requirements")
                        .HasForeignKey("BrokerID");

                    b.HasOne("KWFCI.Models.StaffProfile")
                        .WithMany("Tasks")
                        .HasForeignKey("StaffProfileID");
                });

            modelBuilder.Entity("KWFCI.Models.Message", b =>
                {
                    b.HasOne("KWFCI.Models.StaffProfile", "From")
                        .WithMany()
                        .HasForeignKey("FromStaffProfileID");
                });

            modelBuilder.Entity("KWFCI.Models.StaffProfile", b =>
                {
                    b.HasOne("KWFCI.Models.StaffUser", "User")
                        .WithMany()
                        .HasForeignKey("UserId");
                });

            modelBuilder.Entity("Microsoft.AspNetCore.Identity.EntityFrameworkCore.IdentityRoleClaim<string>", b =>
                {
                    b.HasOne("Microsoft.AspNetCore.Identity.EntityFrameworkCore.IdentityRole")
                        .WithMany("Claims")
                        .HasForeignKey("RoleId")
                        .OnDelete(DeleteBehavior.Cascade);
                });

            modelBuilder.Entity("Microsoft.AspNetCore.Identity.EntityFrameworkCore.IdentityUserClaim<string>", b =>
                {
                    b.HasOne("KWFCI.Models.StaffUser")
                        .WithMany("Claims")
                        .HasForeignKey("UserId")
                        .OnDelete(DeleteBehavior.Cascade);
                });

            modelBuilder.Entity("Microsoft.AspNetCore.Identity.EntityFrameworkCore.IdentityUserLogin<string>", b =>
                {
                    b.HasOne("KWFCI.Models.StaffUser")
                        .WithMany("Logins")
                        .HasForeignKey("UserId")
                        .OnDelete(DeleteBehavior.Cascade);
                });

            modelBuilder.Entity("Microsoft.AspNetCore.Identity.EntityFrameworkCore.IdentityUserRole<string>", b =>
                {
                    b.HasOne("Microsoft.AspNetCore.Identity.EntityFrameworkCore.IdentityRole")
                        .WithMany("Users")
                        .HasForeignKey("RoleId")
                        .OnDelete(DeleteBehavior.Cascade);

                    b.HasOne("KWFCI.Models.StaffUser")
                        .WithMany("Roles")
                        .HasForeignKey("UserId")
                        .OnDelete(DeleteBehavior.Cascade);
                });
        }
    }
}