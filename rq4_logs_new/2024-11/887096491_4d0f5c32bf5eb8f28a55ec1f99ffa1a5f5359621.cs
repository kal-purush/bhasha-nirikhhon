using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using WolfDen.Domain.Entity;

namespace WolfDen.Infrastructure.Configuration
{
    public class LeaveBalanceConfiguration:IEntityTypeConfiguration<LeaveBalance>
    {
        public void Configure(EntityTypeBuilder<LeaveBalance> builder)
        {
            builder.HasOne(x => x.Employee).WithMany().HasForeignKey(x => x.EmployeeId);
            builder.HasOne(X=>X.LeaveType).WithMany().HasForeignKey(x=>x.TypeId);
            builder.Property(x => x.Id).HasColumnName("LeaveBalanceId");
        }
    }
}