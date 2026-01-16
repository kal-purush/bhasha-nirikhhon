using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Home_8
{
    public class PersonCreator
    {

        public Person[] PersonCreates()
        {
            Person[] persons = {
                        new Person("Tom Cruise ", 200000),
                        new Person("John Hill", 200000),
                        new Person("Ben Affleck", 200000)
                         };
            return persons;
        }

    }
}
