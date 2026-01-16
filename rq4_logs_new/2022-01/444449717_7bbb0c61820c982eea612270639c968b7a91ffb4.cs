using Project.ValidationFarmework.validation;
using System;

namespace Project
{
    class Program
    {
        static void Main(string[] args)
        {
            User user = new User("phlonghcmus@gmail.com","Phạm Hoàng Long");
            /*            foreach (var thuoctinh in user.GetType().GetProperties())
                        {
                            Console.WriteLine(thuoctinh.Name);
                            foreach (Attribute attr in thuoctinh.GetCustomAttributes(false))
                            {
                                Console.WriteLine(attr.GetType().Name);
                            }
                        }*/
            Validation validation = new Validation();
            validation.validate(user);
        }
    }
}