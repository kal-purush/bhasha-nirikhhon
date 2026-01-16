using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VTraktate.Core.Interfaces
{
    public interface ICerberosMum
    {
        ICerberos MakeCerberos(ITraktatContext context, Func<int> userIdFunc);
    }
    
}