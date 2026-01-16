using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConfigurationInitializer.Azure
{
    public class AzureRoleConfigurationInitializer : ConfigurationInitializer
    {
        public AzureRoleConfigurationInitializer() : base(new RoleConfigurationReader())
        {
        }
    }
}