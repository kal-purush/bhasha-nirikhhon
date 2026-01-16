using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace modul7_kelompok_3
{
    public class Address
    {
        public string streetAddress { get; set; }
        public string city { get; set; }
        public string state { get; set; }
    }

    public class Course
    {
        public string code { get; set; }
        public string name { get; set; }
    }

    public class DataMahasiswa_2311104060
    {
        public string firstName { get; set; }
        public string lastName { get; set; }
        public string gender { get; set; }
        public int age { get; set; }
        public Address address { get; set; }
        public List<Course> courses { get; set; }

        public void ReadJSON(string path)
        {
            string jsonData = File.ReadAllText(path);
            var mahasiswa = System.Text.Json.JsonSerializer.Deserialize<DataMahasiswa_2311104060>(jsonData);

            Console.WriteLine($"Name     : {mahasiswa.firstName} {mahasiswa.lastName}");
            Console.WriteLine($"Gender   : {mahasiswa.gender}");
            Console.WriteLine($"Age      : {mahasiswa.age}");
            Console.WriteLine($"Address  : {mahasiswa.address.streetAddress}, {mahasiswa.address.city}, {mahasiswa.address.state}");
            Console.WriteLine("Courses  :");

            foreach (var course in mahasiswa.courses)
            {
                Console.WriteLine($"  - {course.code}: {course.name}");
            }
        }
    }

}