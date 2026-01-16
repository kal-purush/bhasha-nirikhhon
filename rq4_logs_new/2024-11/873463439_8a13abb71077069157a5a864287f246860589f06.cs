using Microsoft.EntityFrameworkCore;
using EMS.Domain.Models;
using System;
using EMS.Domain.Models.Account;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.DependencyInjection;

namespace EMS.Infrastructure.Contexts
{
    public class DbSeeding
    {
        public static async Task SeedPermissions(ModelBuilder builder)
        {
            builder.Entity<Permission>().HasData(
                new Permission { Id = "perm_manage_employees", Name = "Manage Employees", Description = "Ability to manage employee records" },
                new Permission { Id = "perm_manage_departments", Name = "Manage Departments", Description = "Ability to manage department records" },
                new Permission { Id = "perm_manage_salary", Name = "Manage Salary", Description = "Ability to manage employee salaries" },
                new Permission { Id = "perm_view_reports", Name = "View Reports", Description = "Ability to view salary and work reports" },
                new Permission { Id = "perm_approve_timecards", Name = "Approve Timecards", Description = "Ability to approve employee timecards" },
                new Permission { Id = "perm_manage_permissions", Name = "Manage Permissions", Description = "Ability to manage user permissions" },
                new Permission { Id = "perm_manage_positions", Name = "Manage Job Positions", Description = "Ability to manage job positions" },
                new Permission { Id = "perm_manage_personal_info", Name = "Manage Personal Info", Description = "Ability to manage personal information" }
            );
        }

        public static async Task SeedRolePermissions(ModelBuilder builder)
        {
            builder.Entity<RolePermission>().HasData(
                // Admin role permissions
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000000", PermissionId = "perm_manage_employees" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000000", PermissionId = "perm_manage_departments" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000000", PermissionId = "perm_manage_salary" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000000", PermissionId = "perm_view_reports" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000000", PermissionId = "perm_approve_timecards" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000000", PermissionId = "perm_manage_permissions" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000000", PermissionId = "perm_manage_positions" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000000", PermissionId = "perm_manage_personal_info" },

                // HR role permissions
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000001", PermissionId = "perm_manage_employees" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000001", PermissionId = "perm_manage_departments" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000001", PermissionId = "perm_manage_salary" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000001", PermissionId = "perm_view_reports" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000001", PermissionId = "perm_approve_timecards" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000001", PermissionId = "perm_manage_positions" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000001", PermissionId = "perm_manage_personal_info" },

                // Department Manager role permissions
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000002", PermissionId = "perm_manage_departments" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000002", PermissionId = "perm_view_reports" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000002", PermissionId = "perm_approve_timecards" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000002", PermissionId = "perm_manage_personal_info" },

                // Employee role permissions
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000003", PermissionId = "perm_manage_personal_info" },
                new RolePermission { RoleId = "00000000-0000-0000-0000-000000000003", PermissionId = "perm_view_reports" }
            );
        }

        public static async Task SeedAdmin(ModelBuilder builder)
        {
            // Static ID for the admin user
            var adminUserId = "00000000-0000-0000-0000-000000000100"; // Static ID for the admin user

            // Create the admin user
            var adminUser = new User
            {
                Id = adminUserId,  // Set the static ID
                UserName = "admin",
                Email = "admin@example.com",
                EmailConfirmed = true,
                NormalizedEmail = "adminUser@EXAMPLE.COM",
                NormalizedUserName = "ADMIN",
            };

            // Hash the password "admin" using PasswordHasher
            var passwordHasher = new PasswordHasher<User>();
            var hashedPassword = passwordHasher.HashPassword(adminUser, "admin"); // Hashing the password "admin"
            adminUser.PasswordHash = hashedPassword;

            // Seed the admin user first
            builder.Entity<User>().HasData(adminUser);

            // Check if the role exists and seed it if necessary

            // Seed the UserRole for the admin user
            builder.Entity<UserRole>().HasData(
                new UserRole
                {
                    RoleId = "00000000-0000-0000-0000-000000000000",   // Reference the seeded role
                    UserId = adminUserId,  // Reference the seeded user
                }
            );
        }



        public static async Task SeedRoles(ModelBuilder builder)
        {
            builder.Entity<Role>().HasData(
                new Role { Id = "00000000-0000-0000-0000-000000000000", Name = "Admin", Description = "ADMIN" },
                new Role { Id = "00000000-0000-0000-0000-000000000001", Name = "HR", Description = "HR" },
                new Role { Id = "00000000-0000-0000-0000-000000000002", Name = "Department Manager", Description = "DEPARTMENT MANAGER" },
                new Role { Id = "00000000-0000-0000-0000-000000000003", Name = "Employee", Description = "EMPLOYEE" }
            );
        }
    }
}