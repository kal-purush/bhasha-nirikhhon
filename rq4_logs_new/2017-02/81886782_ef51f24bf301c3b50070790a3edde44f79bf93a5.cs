using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FoundationsMastery;

namespace FoundationsMastery
{
    public class Computer
    {
        // Sorry for stealing your idea, Kate - I could only think of ideas involving aliens, which would've basically been Zoolandia

        public virtual void Functionality()
        {
            Console.WriteLine("I love to help you get things done!");
        }

        public bool Start()
        {
            return true;
        }
        public virtual bool Shutdown()
        {
            return true;
        }

        public bool HasKeyboard()
        {
            return true;
        }


    }
}