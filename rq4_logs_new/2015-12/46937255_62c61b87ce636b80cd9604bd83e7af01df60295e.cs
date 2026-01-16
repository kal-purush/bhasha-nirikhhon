using Demo.MVC6.Services.Events;
using Infra.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Demo.MVC6.Services
{
    public class MyHandler : IHandler<Base>, IHandler<Derived>
    {
        public Task<bool> HandleAsync(Base e)
        {
            return Task.FromResult(true);
        }

        public Task<bool> HandleAsync(Derived e)
        {
            return Task.FromResult(true);
        }
    }

    namespace Events
    {
        public class Base
        {
        }

        public class Derived : Base
        {
        }
    }

}