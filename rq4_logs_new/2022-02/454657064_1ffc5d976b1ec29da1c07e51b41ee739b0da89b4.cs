using System;
using System.IO;
using System.Net;
using System.Drawing;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using System.Text.RegularExpressions;




namespace CatWorx.BadgeMaker {
    class PeopleFetcher
    {
        //code from GetEmployees() in Program.cs
        public static List<Employee> GetEmployees() 
          {

            List<Employee> employees = new List<Employee>();
            while(true) 
              {
                // Move the initial prompt inside the loop, so it repeats for each employee
                Console.WriteLine("Enter first name (leave empty to exit): ");

                // change input to firstName
                string firstName = Console.ReadLine();
                if (firstName == "") {
                  break;
                }

                // add a Console.ReadLine() for each value
                Console.Write("Enter last name: ");
                string lastName = Console.ReadLine();

                Console.Write("Enter ID: ");
                int id = Int32.Parse(Console.ReadLine());

                Console.Write("Enter Photo URL:");
                string photoUrl = Console.ReadLine();
                
                Employee currentEmployee = new Employee(firstName, lastName, id, photoUrl);
                employees.Add(currentEmployee);
              }

            return employees;
          }
        public static List<Employee> GetFromApi() {
            List<Employee> employees = new List<Employee>();
            using (WebClient client = new WebClient())
            {
                //Image example
                string response = client.DownloadString("https://randomuser.me/api/?results=10&inc=name,id,picture");
                JObject json = JObject.Parse(response);
                foreach (JToken token in json.SelectToken("results")) {
                    // Console.WriteLine(token.SelectToken("name.first"));
                    // Console.WriteLine(token.SelectToken("name.last"));
                    Console.WriteLine(token.SelectToken("id.value"));
                    // Console.WriteLine(token.SelectToken("picture.large"));
                    Regex rgx = new Regex("^[A-Za-z0-9_.]+$");
                    Employee emp = new Employee
                    (
                        token.SelectToken("name.first").ToString(),
                        token.SelectToken("name.last").ToString(),
                        Int32.Parse(token.SelectToken("id.value").ToString().Replace("rgx", "")),
                        token.SelectToken("picture.large").ToString()
                        
                    );
                }
                    // Console.WriteLine(json.SelectToken("results"));
                // }
                
            }
            return employees;
        }
    }
}