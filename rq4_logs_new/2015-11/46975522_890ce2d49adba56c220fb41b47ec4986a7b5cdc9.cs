using System;
using System.ComponentModel;

namespace NotificationAgent.UI.DesignerFixes
{
    internal class AbstractFormDescriptionProvider<TAbstractClass, TClass> : TypeDescriptionProvider
    {
        public AbstractFormDescriptionProvider()
            : base(TypeDescriptor.GetProvider(typeof(TAbstractClass)))
        {
        }

        public override Type GetReflectionType(Type objectType, object instance)
        {
            if (objectType == typeof(TAbstractClass))
            {
                return typeof(TClass);
            }

            return base.GetReflectionType(objectType, instance);
        }

        public override object CreateInstance(IServiceProvider provider, Type objectType, Type[] argTypes, object[] args)
        {
            if (objectType == typeof(TAbstractClass))
            {
                objectType = typeof(TClass);
            }

            return base.CreateInstance(provider, objectType, argTypes, args);
        }
    }
}