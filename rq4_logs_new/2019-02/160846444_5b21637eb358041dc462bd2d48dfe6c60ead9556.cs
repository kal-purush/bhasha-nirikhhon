using System;
using System.IO;

namespace Soccer_Stats {

    class Program {

        static void Main () {

            string currentdirectory = Directory.GetCurrentDirectory();
            DirectoryInfo directory = new DirectoryInfo(currentdirectory);
            var fileName = Path.Combine(directory.FullName, "Data.txt");
            var file = new FileInfo(fileName);
            if (file.Exists){
                using (var reader = new StreamReader(file.FullName)){
                    Console.SetIn(reader);
                    Console.WriteLine(Console.ReadLine());
                }
            }
            else {
                Console.WriteLine("File Missing");
            }

            Console.ReadKey();
        }
    }
}