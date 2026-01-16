using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace FileIOOperations
{
    [Serializable]
    public class Demo
    {
        public string name { get; set; }
        public int age { get; set; }
        public Demo(string name, int age)
        {
            this.name = name;
            this.age = age;
        }
    }
    public class BinaryOperations
    {
        //binary serialize
        public static string filepath = @"C:\Users\Radhika\source\repos\FileIOOperations\FileIOOperations\FileData.txt";
        public static void Serialization()
        {
            Demo demo = new Demo("Radhika", 21);
            FileStream fileStream = new FileStream(filepath, FileMode.Create);
            BinaryFormatter binaryFormatter = new BinaryFormatter();
            binaryFormatter.Serialize(fileStream, demo);

        }
        public static void DeSerialization()
        {
            string binarypath = @"C:\Users\Radhika\source\repos\FileIOOperations\FileIOOperations\FileData.txt";
            FileStream deserialize = new FileStream(binarypath, FileMode.Open);
            BinaryFormatter binaryFormatter1 = new BinaryFormatter();
            Demo d = (Demo)binaryFormatter1.Deserialize(deserialize);
            Console.WriteLine("Name: {0} \t Age: {1}", d.name, d.age);
            //Serialization();
        }
    }
    }