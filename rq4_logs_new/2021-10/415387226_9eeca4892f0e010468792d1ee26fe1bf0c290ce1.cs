using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using Home2Med.Shared.Entity;
using System;

namespace Home2Med.Server
{
    public class ApplicationDbContext:DbContext
    {
      /* Cada DbSet es una tabla que crearemos a partir de una entidad */
      public DbSet<Doctor> Doctors {get;set;}
      public DbSet<Nurse> Nurses {get;set;}
      public DbSet<Patient> Patients {get;set;}
      public DbSet<Relative> Relatives {get;set;}
      public ApplicationDbContext(DbContextOptions<ApplicationDbContext>options):base(options){
      }
      protected override void OnModelCreating(ModelBuilder modelBuilder){
         /* Establecemos los tipos de relaciones entre las tablas que se van a crear 1 - 1, 1 - *, N - M Creamos una llave primaria compuesta para la tabla CategoryMovie*/
         modelBuilder.Entity<Doctor>().HasKey(x=> new {x.PatientId});
         base.OnModelCreating(modelBuilder);
      }
   }
}