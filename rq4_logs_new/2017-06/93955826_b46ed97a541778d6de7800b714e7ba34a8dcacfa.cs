using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PropConsoleApp
{
    class Employee
    {
        

        // Field data.
        private string empName;
        private int empID;
        private int empAge;

        // Constructors.
        public Employee() { }

        /// <summary>
        /// Используем свойства в конструкторе, чтобы в случае установки параметров так же делались определенные проверки. 
        /// При использовании полей - можно добавить любое значение !!!
        /// </summary>
        public Employee(string name, int age, int id, float pay)
        {
            Name = name;
            Age = age;
            ID = id;
            Pay = pay;
        }
        // Methods.
        public void GiveBonus(float amount)
        { Pay += amount; }

        // Properties!
        public string Name
        {
            get { return empName; }
            set
            {
                if (value.Length > 15)
                    Console.WriteLine("Error! Name length exceeds 15 characters!");
                else
                    empName = value;
            }
        }

        public int ID
        {
            get { return empID; }
            set { empID = value; }
        }
        // Укороченная завись
        public float Pay { get; set; }

        // Обычное свой-ство
        public int Age
        {
            get { return empAge; }
            set { empAge = value; }
        }

        

    }
}
