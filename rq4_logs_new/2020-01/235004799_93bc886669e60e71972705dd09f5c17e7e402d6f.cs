using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MLReporting.Domain.Model;

namespace MLReporting.Domain.Abstract
{
    public interface ITenantRepository
    {
        IEnumerable<UserRoleDim> GetUsers();

        IEnumerable<VirtualMachineBaseUsageHourlyFact> GetVMHourlyUsageByUserId(int id, int startDate, int endDate = 0);
        
        VirtualMachineBaseUsageHourlyFact GetDateLastModified(int id);

        IEnumerable<VirtualMachineDim> GetVMsByUserId(int id);
    }
}