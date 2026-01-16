using GraduateProject.Common.Models;
using GraduateProject.Common.Models.SysModels;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraduateProject.Common.Data
{
    public class ApplicationDbContext : IdentityDbContext<ApplicationUser>
    {
        public ApplicationDbContext()
        {
        }

        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options)
            : base(options)
        {
        }

        public virtual DbSet<Department> Departments { get; set; }
        public virtual DbSet<DepartmentTranslate> DepartmentTranslates { get; set; }
        public virtual DbSet<Doctor> Doctors { get; set; }
        public virtual DbSet<DoctorTranslate> DoctorTranslates { get; set; }
        public virtual DbSet<Hotel> Hotels { get; set; }
        public virtual DbSet<HotelTranslate> HotelTranslates { get; set; }
        public virtual DbSet<MedicalCenter> MedicalCenters { get; set; }
        public virtual DbSet<MedicalCenterTranslate> MedicalCenterTranslates { get; set; }
        public virtual DbSet<PlaceToVisit> PlaceToVisits { get; set; }
        public virtual DbSet<PlaceToVisitTranslate> PlaceToVisitTranslates { get; set; }
        public virtual DbSet<Review> Reviews { get; set; }
        public virtual DbSet<ContactUs> ContactUs { get; set; }
        public virtual DbSet<SysCity> SysCities { get; set; }
        public virtual DbSet<SysPlaceType> SysPlaceTypes { get; set; }

    }
}