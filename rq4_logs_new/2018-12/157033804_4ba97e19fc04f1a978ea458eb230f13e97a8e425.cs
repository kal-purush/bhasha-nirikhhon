using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fishezzz
{
    public class Exceptions
    {
        /// <summary>
        /// Exception thrown when the trip parameter is not of the type Trip, whilst converting a Trip into a TripExtra
        /// </summary>
        public class NotATripException : ApplicationException
        {
            public NotATripException()
                : base()
            { }

            public NotATripException(string message)
                : base(message)
            { }

            public NotATripException(string message, Exception innerException)
                : base(message, innerException)
            { }
        }

        /// <summary>
        /// Exception thrown when the route parameter is nit of the type Route, whilst converting a Route into a RouteExtra
        /// </summary>
        public class NotARouteException : Exception
        {
            public NotARouteException()
                : base()
            { }

            public NotARouteException(string message)
                : base(message)
            { }

            public NotARouteException(string message, Exception innerException)
                : base(message, innerException)
            { }
        }
    }
}