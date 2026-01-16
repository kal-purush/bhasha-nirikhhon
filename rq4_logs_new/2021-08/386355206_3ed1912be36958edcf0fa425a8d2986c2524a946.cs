using System;

namespace Abstraction
{
    public abstract class Animal
    {
        protected abstract string Name { get; }
        public abstract void MakeSound();
        public void Run(Animal animal) => Console.WriteLine($"The {animal.Name} is running.");
    }

    public class Dog : Animal
    {
        protected override string Name => nameof(Dog);
        public override void MakeSound() => Console.WriteLine("Woof - Woof");
        public void BringStick() => Console.WriteLine("Bring a stick");
    }

    public class Cat : Animal
    {
        protected override string Name => nameof(Cat);
        public override void MakeSound() => Console.WriteLine("Meow - meow");
        public void CatchMouse() => Console.WriteLine("Catch the mouse");
    }
}