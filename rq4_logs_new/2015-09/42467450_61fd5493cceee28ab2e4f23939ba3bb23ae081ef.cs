using System;
using System.Reflection;

namespace Browser.xUnit
{
    [AttributeUsage(AttributeTargets.Assembly)]
    public class BeforeAfterAssemblyTestsAttribute : Attribute
    {
        /// <summary>
        /// The type of the class that is to be instantiated to handle assembly before and after events.
        /// </summary>
        public Type HandlerType { get; set; }

        /// <summary>
        /// Initializes a new instances of <see cref="BeforeAfterAssemblyTestsAttribute"/>
        /// </summary>
        public BeforeAfterAssemblyTestsAttribute(Type handlerType)
        {
            if (!typeof(IBeforeAfterAssemblyTests).IsAssignableFrom(handlerType))
            {
                throw new ArgumentException(
                    $"Handler type '{handlerType.Name}' must implement {nameof(IBeforeAfterAssemblyTests)}!",
                    nameof(handlerType));
            }
            HandlerType = handlerType;
        }
    }
}