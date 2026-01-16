using System;

namespace Shared
{
    [Serializable]
    public class Client
    {
        private string name;
        private string cpf;
        private int age;
        private string phone;
        private string address;
        private string email;

        public Client(string name, string cpf, int age, string phone, string address, string email)
        {
            this.name = name;
            this.cpf = cpf;
            this.age = age;
            this.phone = phone;
            this.address = address;
            this.email = email;
        }
        
        public string Name
        {
            get { return name; }
            set { name = value; }
        }

        public string Cpf
        {
            get { return cpf; }
            set { cpf = value; }
        }

        public int Age
        {
            get { return age; }
            set { age = value; }
        }

        public string Phone
        {
            get { return phone; }
            set { phone = value; }
        }

        public string Address
        {
            get { return address; }
            set { address = value; }
        }

        public string Email
        {
            get { return email; }
            set { email = value; }
        }

    }
}