using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QTFK.Extensions.Exceptions
{
    public static class ExceptionExtension
    {
        public static T wrap<T>(this Exception exception, Func<Exception, T> wrapperDelegate) where T : Exception
        {
            T wrappedException;

            Asserts.isSomething(wrapperDelegate, $"'{nameof(wrapperDelegate)}' cannot be null.");

            wrappedException = wrapperDelegate(exception);
            Asserts.isSomething(wrappedException, $"'{nameof(wrapperDelegate)}' must return a not null Exception.");

            return wrappedException;
        }
    }
}