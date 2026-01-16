using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using System.Text.Json;
using System.IO;

namespace Model
{
    public class Employee
    {
        //автоматические свойства        
        public string Name { get; set; }
        public string Position { get; set; }
        public string Describe { get; set; }

        private DateTime birthDay;

        
        public DateTime BirthDay
        {
            get
            {
                return birthDay;
            }
            set
            {
                if (value>DateTime.Now) throw new ArgumentException("День рождения не может быть больше сегодняшнего дня");
                birthDay = value;
            }
        }

        public Employee(string name, string position, string describe, DateTime birthDay)
        {
            Name = name;
            Position = position;
            Describe = describe;
            
            BirthDay = birthDay;            
        }

        public Employee()//для сериализация
        {
          
        }

        

    }


    public class Employees//Database
    {
         List<Employee> list;

        int index;

        public int CurrentIndex
        {
            get { return index; }
        }

        public void Next()
        {
            if (list.Count>index+1) 
                index++;
        }

        public void Prev()
        {
            if (index>0) index--;
        }

        public void Remove()
        {
            list.RemoveAt(index);
            Prev();
        }

        public Employees()
        {
            list = new List<Employee>();
            index = -1;
        }

        public Employee CurrentEmployee
        {
            get
            {
                try
                {
                    return list[index];
                }
                catch
                {
                    return null;
                }
            }
        }

        public void Save(string fileName)
        {
            XmlSerializer xmlSerializer = new XmlSerializer(typeof(List<Employee>));
            Stream fStream = new FileStream(fileName, FileMode.Create, FileAccess.Write);
            xmlSerializer.Serialize(fStream, list);
            fStream.Close();

        }


        public void Add(Employee employeer)
        {
            list.Add(employeer);
        }
        public void SaveJSON(string fileName)
        {
           File.WriteAllText(fileName, JsonSerializer.Serialize(list));

        }
      

        public void Load(string fileName)
        {

            XmlSerializer xmlSerializer = new XmlSerializer(typeof(List<Employee>));
            Stream fStream = new FileStream(fileName, FileMode.Open, FileAccess.Read);
            list=(List<Employee>)xmlSerializer.Deserialize(fStream);
            fStream.Close();
        }
    }
}