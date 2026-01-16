using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace cr2
{
    class SuperHashSet<T> : HashSet<T> where T : struct
    {

    }

    public delegate void EventHandler();

    class User
    {

        public event EventHandler Click;
        public void ComandClick()
        {
            Console.WriteLine("Click!");
            if (Click != null)
                Click();
        }
    }

    class Button
    {
        public void OnClick()
        {
            Console.WriteLine("На меня нажали");
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("---1 задание");
            SuperHashSet<int> SHS = new SuperHashSet<int>();
            SHS.Add(4);
            SHS.Add(4);
            SHS.Add(5);
            SHS.Add(2);
            SHS.Add(1);

            foreach (int i in SHS)
            {
                Console.WriteLine(i);
            }

            Console.WriteLine("---2 задание");
            int n = 5;
            var count = from c in SHS
                        where c == n
                        select c;
            Console.WriteLine(count.Count());

            Console.WriteLine("---3 задание");
            User user = new User();
            Button butt1 = new Button();
            Button butt2 = new Button();
            user.Click += new EventHandler(butt1.OnClick);
            user.Click += new EventHandler(butt2.OnClick);
            user.ComandClick();
        }
    }
}