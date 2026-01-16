namespace Practice3.Task1
{
    class Student 
    {
        public string Name;
        public int Age;
        public void Write()
        {
            Console.WriteLine($"Студент {Name} - {Age} лет");
        }
    }
    internal class Program
    {
        static void Main(string[] args)
        {
            Student Sveta = new Student();
            Sveta.Name = "Света";
            Sveta.Age = 46;
            Sveta.Write();
            //Console.WriteLine("Hello, World!");
        }
    }
}