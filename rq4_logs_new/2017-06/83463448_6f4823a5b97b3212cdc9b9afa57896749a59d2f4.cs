using System;
using System.Linq;
using System.Reflection;
using Autofac.Features.ResolveAnything;
using Lykke.JobTriggers.Triggers.Attributes;

namespace Lykke.JobTriggers.Autofac
{
    public class JobFunctionsNotAlreadyRegisteredSource : AnyConcreteTypeNotAlreadyRegisteredSource
    {
        public JobFunctionsNotAlreadyRegisteredSource() : 
            base(FilterJobFunctionsPredicate)
        {
        }

        private static bool FilterJobFunctionsPredicate(Type t)
        {
            return t.GetTypeInfo()
                .GetMethods(BindingFlags.Instance | BindingFlags.Public)
                .Any(m =>
                    m.GetCustomAttribute<TimerTriggerAttribute>() != null ||
                    m.GetCustomAttribute<QueueTriggerAttribute>() != null);
        }
    }
}