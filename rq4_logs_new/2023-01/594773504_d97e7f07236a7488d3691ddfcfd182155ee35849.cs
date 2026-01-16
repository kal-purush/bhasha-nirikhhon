using libraryBookwithcard.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace libraryBookwithcard
{
    internal class ControllerBooks
    {
        private List<book> books;

        public ControllerBooks() { 
        
        books= new List<book>();
            load();
        
        }

        public void load()
        {
            string path = Application.StartupPath + @"/data/books.txt";

            StreamReader streamReader = new StreamReader(path);

            string t;

            while ((t = streamReader.ReadLine()) != null)
            {

                book a = new book(t);

                books.Add(a);
            }

            streamReader.Close();
        }

        public void afisare()
        {

            for(int i=0;i<books.Count;i++)
            {
                MessageBox.Show(books[i].descriere());
            }

        }




    }
}