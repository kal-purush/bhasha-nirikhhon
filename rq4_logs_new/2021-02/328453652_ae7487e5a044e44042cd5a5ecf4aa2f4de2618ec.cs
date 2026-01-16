namespace EF.Practice.CodeFirst.Contexts
{
    using EF.Practice.CodeFirst.Entities;
    using Microsoft.EntityFrameworkCore;

    public class EmployeeDbContext : DbContext
    {
        public EmployeeDbContext()
        {
        }

        public DbSet<Employee> Employees { get; set; }

        public DbSet<EmployeePayHistory> EmployeePayHistory { get; set; }

        public DbSet<JobCandidate> JobCandidates { get; set; }

        public DbSet<SalesPerson> SalesPeople { get; set; }

        public DbSet<Shift> Shifts { get; set; }

        public DbSet<Department> Departments { get; set; }

        public DbSet<Person> People { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Employee>().HasIndex(emp => emp.NationalIDNumber).IsUnique();
            modelBuilder.Entity<Employee>().HasIndex(emp => emp.LoginID).IsUnique();

            modelBuilder.Entity<EmployeeDepartmentShift>().HasKey(x => new { x.BusinessEntityId, x.DepartmentId, x.ShiftId, x.StartDate });

            modelBuilder.Entity<EmployeeDepartmentShift>()
                .HasOne(eds => eds.Shift)
                .WithMany(sh => sh.EmployeeDepartmentShifts)
                .HasForeignKey(eds => eds.ShiftId);

            modelBuilder.Entity<EmployeeDepartmentShift>()
                .HasOne(eds => eds.Department)
                .WithMany(dp => dp.EmployeeDepartmentShifts)
                .HasForeignKey(eds => eds.DepartmentId);

            modelBuilder.Entity<EmployeeDepartmentShift>()
                .HasOne(eds => eds.Employee)
                .WithMany(emp => emp.EmployeeDepartmentShifts)
                .HasForeignKey(eds => eds.BusinessEntityId);

            modelBuilder.Entity<Shift>().HasIndex(sh => sh.Name).IsUnique();
            modelBuilder.Entity<Shift>().HasIndex(sh => sh.StartTime).IsUnique();
            modelBuilder.Entity<Shift>().HasIndex(sh => sh.EndTime).IsUnique();

            modelBuilder.Entity<Department>().HasIndex(dp => dp.Name).IsUnique();

            modelBuilder.Entity<EmployeePayHistory>()
                .HasOne(eph => eph.Employee)
                .WithMany(emp => emp.History)
                .HasForeignKey(eph => eph.BusinessEntityId);

            modelBuilder.Entity<Employee>()
                .HasOne(emp => emp.Person)
                .WithMany(p => p.Employees)
                .HasForeignKey(emp => emp.PersonId);

            modelBuilder.Entity<SalesPerson>()
                .HasOne(sp => sp.Employee)
                .WithMany(emp => emp.SalesPeople)
                .HasForeignKey(sp => sp.BusinessEntityId);

            modelBuilder.Entity<JobCandidate>()
                .HasOne(jc => jc.Employee)
                .WithMany(emp => emp.JobCandidates)
                .HasForeignKey(jc => jc.BusinessEntityId);
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer("Data Source=.\\SQLEXPRESS;Initial Catalog=EmployeeDB;Integrated Security=true");
        }
    }
}