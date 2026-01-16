namespace Inertia
{
    abstract class Element
    {
        public int X { get; set; }
        public int Y { get; set; }
        public string Gist { get; set; }

        public abstract void Display();
    }
}