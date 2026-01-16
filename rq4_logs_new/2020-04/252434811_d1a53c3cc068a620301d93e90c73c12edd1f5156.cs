using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeWork
{
    class TrelloAnalog
    {

    }

    public enum BoardStatus
    {
        ToDo,
        OnTeacher,
        OnStudent,
        Done
    }

    class Person
    {
        public static int idCount;

        public int id;
        public string PersonName { get; set; }
        
        public Person(string personName)
        {
            id = ++idCount;    
            PersonName = personName;
        }
        public Person(string personName, int idExist)
        {
            id = idExist;
            PersonName = personName;
        }
    }

    class Board
    {
        public static int idCount;

        int id;

        //public List<>
    }

    class HomeWork
    {
        public static int idCount;

        int id;

        Person Student;
    }
}