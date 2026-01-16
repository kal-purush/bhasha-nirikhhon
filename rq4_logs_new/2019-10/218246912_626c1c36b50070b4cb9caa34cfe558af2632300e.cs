using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ParseTo
{
    public abstract class Handler
    {
        protected abstract IEnumerable<Assembly> LoadExtensibleAssemblies();

        private IEnumerable<Assembly> getAssemblies<T>()
        {
            var assemblies = new List<Assembly> { Assembly.GetExecutingAssembly(), typeof(T).Assembly };

            assemblies.AddRange(LoadExtensibleAssemblies());

            return assemblies;
        }

        private static IParseTo<T> createParser<T>(Type parserType)
        {
            return (IParseTo<T>)Activator.CreateInstance(parserType ?? throw new InvalidOperationException($"You are trying to create an instance of { (Type)null }"));
        }

        private IParseTo<T> getParser<T>()
        {
            if(typeof(T).GetInterfaces().Any(i => i == typeof(IParseTo<T>)))
                return createParser<T>(typeof(T));
            
            var parserType = getAssemblies<T>().Select(a => a.GetTypes().FirstOrDefault(type => typeof(IParseTo<T>).GetTypeInfo().IsAssignableFrom(type))).FirstOrDefault();

            return createParser<T>(parserType);
        }

        private static bool isNullable<T>()
        {
            return Nullable.GetUnderlyingType(typeof(T)) != null;
        }

        private static T returnDefault<T>(IParseTo<T> parser, object defaultValue)
        {
            if (defaultValue == default)
                return parser.GetDefault();

            return (T)defaultValue;
        }

        public T Handle<T>(object i, object defaultValue)
        {
            var parser = getParser<T>();

            object result = null;
            try
            {
                result = parser.Parse(i);
            }
            catch (Exception)
            {
                if (isNullable<T>())
                    return returnDefault(parser, defaultValue);
            }
            
            if (isNullable<T>() || result != null)
                return (T)result;

            return returnDefault(parser, defaultValue);
        }
    }
}