using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;

namespace MAJServices.Entities
{
    public static class InfoContextSeeds
    {
        public static void SeedData(ModelBuilder modelBuilder)
        {
            seedDepartments(modelBuilder);
            seedRoles(modelBuilder);
            seedUsers(modelBuilder);
        }

        private static void seedRoles(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<RoleIdentity>().HasData(
                new RoleIdentity
                {
                    Name = "SuperAdmin"
                },
                new RoleIdentity
                {
                    Name = "Admin"
                },
                new RoleIdentity
                {
                    Name = "Subadmin"
                },
                new RoleIdentity
                {
                    Name = "General"
                });
        }

        private static void seedUsers(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<UserIdentity>().HasData(
                new UserIdentity
                {
                    Id = "2014630132",
                    UserName = "2014630132",
                    Name = "Arturo",
                    LastName = "Escutia López",
                    DepartmentAcronym = "CATT",
                },
                 new UserIdentity
                 {
                     Id = "2014378223",
                     UserName = "2014378223",
                     Name = "Javier",
                     LastName = "Cruz Santiago",
                     DepartmentAcronym = "CELEX"
                 },
                 new UserIdentity
                 {
                     Id = "2014631903",
                     UserName = "2014631903",
                     Name = "Miguel",
                     LastName = "Medina Zarazúa",
                     DepartmentAcronym = "UPIS"
                 },
                 new UserIdentity
                 {
                     Id = "2014193056",
                     UserName = "2014193056",
                     Name = "Axel",
                     LastName = "Servantes Vargas",
                     DepartmentAcronym = "GE"
                 });
        }

        private static void seedDepartments(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Department>().HasData(
                new Department
                {
                    Name = "Centro de Lenguajes Extranjeras",
                    Acronym = "CELEX"
                },
                 new Department
                 {
                     Name = "Comisión Académica de Trabajos Terminales",
                     Acronym = "CATT"
                 },
                 new Department
                 {
                     Name = "Gestión Escolar",
                     Acronym = "GE"
                 },
                 new Department
                 {
                     Name = "Unidad Politécnica de Integración Social",
                     Acronym = "UPIS"
                 },
                 new Department
                 {
                     Name = "Departamento de Extensión y Apoyos Educativos",
                     Acronym = "DEAE"
                 });
        }

    }
}