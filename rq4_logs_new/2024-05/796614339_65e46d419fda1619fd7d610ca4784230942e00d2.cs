using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LibraryManager
{
    internal class Student
    {
        private int std_id;
        private string std_name;
        private string std_email;
        private int std_no; //kártyaszám
        private string password;
        private string std_bdate;
        private string contact; //telefonszám

        public int Std_id { get => std_id; set => std_id = value; }
        public string Std_name { get => std_name; set => std_name = value; }
        public string Std_email { get => std_email; set => std_email = value; }
        public int Std_no { get => std_no; set => std_no = value; }
        public string Password { get => password; private set => password = value; }
        public string Std_bdate { get => std_bdate; set => std_bdate = value; }
        public string Contact { get => contact; set => contact = value; }

        public Student(int std_id, string std_name, string std_email, int std_no, string password, string std_bdate, string contact)
        {
            Std_id = std_id;
            Std_name = std_name;
            Std_email = std_email;
            Std_no = std_no;
            Password = password;
            Std_bdate = std_bdate;
            Contact = contact;

            StreamWriter sw = new StreamWriter("students.txt", true);
            sw.WriteLine($"{std_id}|{std_name}|{std_email}|{std_no}|{password}|{std_bdate}|{contact}");
            sw.Close();
        }
    }
}