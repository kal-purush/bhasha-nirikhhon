using ColdPlayProject.list_in_csharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ColdPlayProject.list_in_csharp
{
    public class HouseHold
    {
 
        private IList<string> items;
        private IList<int> _studentAge;
        private IList<Toys> toys;  

        public HouseHold()
        {
            items = new List<string>();
            items.Add("Bob Goblin");
            items.Add("Gina");
            items.Add("Nobel");
            items.Add("HogaDog");
            items.Add("Walykassam");
            items.Add("Kite");
            items.Insert(1, "Floggy Long Legs");
        }


        public void ShowAll()
        {
            int index = items.IndexOf("Walykassam");
        }


        public void ShowStudentAge()
        {
            _studentAge.Add(55);
        }


        public void MakeToy()
        {
            Toys toy = new Toys();
            toy.ToyName = "Timon";
            toy.ToyAge = 12;
            toy.ToyPrice = 11.02;

            toys = new List<Toys>(); 
            toys.Add(toy);
        }
    }
}