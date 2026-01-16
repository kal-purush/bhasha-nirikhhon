using System;

namespace LearningCSharp.Lessons
{
    public class Structs
    {
        [Demo]
        public static void ShowReadOnlyStructEvilness()
        {
            StructsMutability mutability = new StructsMutability();

            Console.WriteLine("Incrementing Field 2 times, initial value = 1");
            Console.WriteLine(mutability.fieldIncrement.Increment());
            Console.WriteLine(mutability.fieldIncrement.Increment());
            
            Console.WriteLine("Incrementing Property 2 times, initial value = 1");
            Console.WriteLine(mutability.properyIncrement.Increment());
            Console.WriteLine(mutability.properyIncrement.Increment());
            
            Console.WriteLine("Incrementing Readonly Field 2 times, initial value = 1");
            Console.WriteLine(mutability.readonlyFieldIncrement.Increment());
            Console.WriteLine(mutability.readonlyFieldIncrement.Increment());
        }
    }
    
    
    public class StructsMutability
    {
        public  NumberIncrement fieldIncrement;
        public  NumberIncrement properyIncrement { get; }
        public readonly NumberIncrement readonlyFieldIncrement;

        public StructsMutability()
        {
            this.fieldIncrement = new NumberIncrement(1);
            this.properyIncrement = new NumberIncrement(1);
            this.readonlyFieldIncrement = new NumberIncrement(1);
        }
    }

    public struct NumberIncrement
    {
        private int _number;

        public NumberIncrement(int number) : this()
        {
            this._number = number;
        }

        public int Increment()
        {
            this._number++;
            return this._number;
        }
    }
}