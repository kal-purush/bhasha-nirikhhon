using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Security.Cryptography;

namespace Lab02_DataEncription
{
    class Program
    {
        static void Main(string[] args)
        {
            DirectoryInfo f;

            string filePath= "", entry= "", type= "";
            bool exit = true;
            while (exit)
            {
                RSA rsa = new RSA();
                Console.WriteLine("Write your user to generate key:");
                Console.SetCursorPosition(32, Console.CursorTop - 1);
                var user =  Console.ReadLine();
                rsa.GenerateKeys();
                Console.WriteLine("your private key is:" + rsa.j.ToString());
               // rsa.Encryption();
                Console.WriteLine("c:/encryption/:");
                Console.SetCursorPosition(15, Console.CursorTop - 1);
                entry = Console.ReadLine();
                f = new DirectoryInfo(entry);
                if (!Validation(entry, ref filePath, ref type))
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("¡Error! Asegúrese de haber escrito correctamente los comandos del progama. Verifique que exista el archivo.");
                    Console.ForegroundColor = ConsoleColor.White;
                }
                else
                {
                    //Here goes RSA
                   // exit = true;
                }

                
            }
            Console.ReadLine();
           /* byte[] bytes;
            using (var file = new FileStream("C:\\Users\\jsala\\Pictures\\emilyo.jpeg", FileMode.Open))
            {
                using (var binaryFile = new BinaryReader(file, Encoding.ASCII))
                {
                    var mensaje = binaryFile.ReadBytes((int)file.Length);
                    // Instanciamos el algorimo asimétrico RSA

                    bytes = mensaje;
                }
            }
            using (var file = new FileStream("C:\\Users\\jsala\\Pictures\\emilyo.txt", FileMode.Append))
            {
                using (var binaryFile = new BinaryWriter(file, Encoding.ASCII))
                {
                   
                    RSACryptoServiceProvider primerRSA = new RSACryptoServiceProvider();
                    // Establecemos la longitud de la clave que queremos usar
                    primerRSA.KeySize = 2048;
                   
                    bytes = primerRSA.Encrypt(bytes, true);
                    for (int i = 0; i < bytes.Length; i++)
                    {
                        binaryFile.Write(bytes[i]);
                    }

                }
            }*/
            
           
          
        }
        //Validation
        static bool Validation(string entry, ref string filePath, ref string type)
        {
            string[] current = entry.Split(' ');
            if (current[0].ToLower() == "exit")
            {
                type = "exit";
                return true;
            }
            else
            {
                if (!(current.Length > 2))
                {
                    if (!(current[0] == "-c0" || current[0] == "-d" || current[0] == "-c1"))
                        return false;
                    type = current[0];
                    string[] path = current[1].Split('"');
                    if (!(path.Length > 3))
                    {
                        if (!(path[0] == "-f" && File.Exists(path[1])))
                            return false;
                        filePath = path[1];
                    }
                    else
                        return false;
                    return true;

                }
                else
                    return false;
            }

        }
    }
}