using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DIMU.BLL.ServiceInterfaces
{
    public interface IAdminService
    {
        public Task<String> Hello();
    }
}