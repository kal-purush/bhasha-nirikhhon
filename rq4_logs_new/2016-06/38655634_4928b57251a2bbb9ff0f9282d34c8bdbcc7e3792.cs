using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hatfield.EnviroData.DataProfile.WQ
{
    public class DomainBuildResult<T>
    {
        public System.Data.Entity.EntityState State { get; set; }
        public T Data { get; set; }

        public DomainBuildResult(T data, System.Data.Entity.EntityState state)
        {
            Data = data;
            State = state;
        }
    }
}