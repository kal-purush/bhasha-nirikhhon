using System;

namespace EventsExample
{
    public delegate void Action(string message);
    class classA
    {
        public classA(classB classb)
        {
            classb.lessonEvent += new Action(Classb_lessonEvent); // подписка на эвент
        }

        private void Classb_lessonEvent(string message)
        {
            Console.WriteLine($"Эвент сработал, внутри класса А! НОМЕР {message}");
        }
    }

    class classC
    {
        public classC(classB classb)
        {
            classb.lessonEvent += new Action(Classb_lessonEvent); // подписка на эвент
        }

        private void Classb_lessonEvent(string message)
        {
            Console.WriteLine($"Эвент сработал, внутри класса C! НОМЕР {message}");
        }
    }
    class classB
    {
        public event Action lessonEvent;
        public void Work()
        {
            Random rand = new Random();
            for (int i = 0; i < 100000; i++)
            {
                if (rand.Next(1, 100000) == 228)
                {
                    Console.WriteLine(lessonEvent.GetInvocationList().Length);
                    lessonEvent?.Invoke(i.ToString()); //lessonEvent();
                }
            }
            //Console.WriteLine("");
        }
    }
    class Program
    {
        static void Main(string[] args)
        {
            classB b = new classB();
            classA a = new classA(b);
            classC c = new classC(b);
            classA a1 = new classA(b);
            b.Work();
        }
    }
}