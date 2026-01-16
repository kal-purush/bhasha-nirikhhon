using DesignPatternPartTwo.Prototype;
using DesignPatternPartTwo.Singleton;

namespace DesignPatternPartTwo
{
    internal class Program
    {
        static void Main(string[] args)
        {
            //Console.WriteLine("Hello, World!");

            //ClientCodeForUsingPrototype.UseWithoutPrototype();

            //ClientCodeForUsingPrototype.UseWithPrototype();

            //ClientCodeForUsingPrototype.UseShallowVsDeepCopy();

            //ClientCodeSingle.UseSingletonWithoutThreadSafeAndLazyLoading();

            ClientCodeSingle.UseMultiThreadProgram();

        }
    }
}