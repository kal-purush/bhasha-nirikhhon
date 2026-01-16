using System;
using System.Collections.Generic;
using System.Text;

namespace chapter_08.domain.service._667
{
    class boy667
    {
        public int final_MIN_AGE = 0;
        public int final_MAX_AGE = 100;
        public string name;
        public int age;
        public string getName()
        {
            return this.name;
        }
        public string SetName(string name)
        {
            if (name == "")
            {
                Console.WriteLine("引数エラーです");
                this.name = name;
            }
            else
            {
                this.name = name;
            }
            return this.name;
        }
        public int GetAge()
        {
            return this.age;
        }
        public int SetAge(int age)
        {
            if(age < final_MIN_AGE || age > final_MAX_AGE)
            {
                Console.WriteLine("引数エラーです");
                this.age = age;
            }
            else
            {
                this.age = age;
            }
            return this.age;
        }
    }
}