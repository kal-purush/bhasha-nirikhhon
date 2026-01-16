using BBK.App.DataAccess;
using LightMock;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BBK.App.Tests.Mocks
{
    /// <summary>
    /// Extension methods for use with the MockContext for creating mock instances.
    /// </summary>
    public static class MockContextExtensions
    {
        private static Dictionary<object, WeakReference<object>> _references = new Dictionary<Type, WeakReference<object>>();


        public static IBasicDataAccess CreateInstance(this MockContext<IBasicDataAccess> context)
        {
            _references
            return new MockIBasicDataAccess(context);
        }
    }
}