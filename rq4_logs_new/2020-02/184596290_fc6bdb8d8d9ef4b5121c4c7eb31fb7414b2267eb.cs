using System;

namespace LearningCSharp.Lessons
{
    public class ObjectOriented
    {
        [Demo]
        public static void PolymorphMethodDefaultValue()
        {
            BaseDefault baseDefault = new ChildDefault();
            baseDefault.MethodWithDefaultValue();
        }
    }

    public class BaseDefault
    {
        public virtual void MethodWithDefaultValue(int defaultValue = 5)
        {
            Console.WriteLine($"Call Method in BaseDefault class, and default value is {defaultValue}");
        }
    }

    public class ChildDefault : BaseDefault
    {
        public override void MethodWithDefaultValue(int defaultValue = 7)
        {
            Console.WriteLine($"Call Method in ChildDefault class, and default value is {defaultValue}");

        }
    }
}