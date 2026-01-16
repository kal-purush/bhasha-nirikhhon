using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace QTFK.Extensions.Types
{
    public static class TypeExtensions
    {
        //public static bool implementsInterface(this Type type, string interfaceTypeFullName)
        //{
        //    bool implementsInterfaceResult;
        //    Type interfaceType;

        //    interfaceType = prv_getType(interfaceTypeFullName);
        //    implementsInterfaceResult = prv_implementsInterface(type, interfaceType);

        //    return implementsInterfaceResult;
        //}

        public static bool implementsInterface(this Type type, Type interfaceType)
        {
            return prv_implementsInterface(type, interfaceType);
        }

        public static bool implementsInterface<T>(this Type type) where T : class
        {
            return prv_implementsInterface(type, typeof(T));
        }

        private static Type prv_getType(string interfaceTypeFullName)
        {
            Type interfaceType;

            Asserts.isFilled(interfaceTypeFullName, $"Parameter '{nameof(interfaceTypeFullName)} cannot be empty.'");

            
            interfaceType = Type.GetType(interfaceTypeFullName);
            Asserts.isSomething(interfaceType, $"Could not load Type for '{interfaceTypeFullName}'");

            return interfaceType;
        }

        private static bool prv_implementsInterface(Type type, Type interfaceType)
        {

            Asserts.isSomething(type, "Parameter 'type' cannot be null.");
            Asserts.isSomething(interfaceType, "Parameter 'interfaceType' cannot be null.");
            Asserts.check(type.IsInterface == false, "Parameter 'type' cannot be an interface.");
            Asserts.check(interfaceType.IsInterface == true, "Parameter 'interfaceType' must be an interface.");

            return type
                .GetInterfaces()
                .Any(t => t.Equals(interfaceType))
                ;
        }
    }
}