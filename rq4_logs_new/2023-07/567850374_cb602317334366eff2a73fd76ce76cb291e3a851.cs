using System.IO;

namespace PraticasComArquivos
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            string sourcePath = @"C:\Estudos\C#\EstudosCSharp\PraticasComArquivos\PraticasComArquivos\src\Names.txt";

            //FileStream fs = null;
            StreamReader sr = null;

            try
            {
                //fs = new FileStream(sourcePath, FileMode.Open);
                sr = File.OpenText(sourcePath); // Forma mais limpa 
                string s = sr.ReadToEnd();

                Console.WriteLine(s);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Deu Ruim!");
            }
            finally
            {
                //if (fs != null) fs.Close();
                if (sr != null) sr.Close();
            }

        }
    }
}