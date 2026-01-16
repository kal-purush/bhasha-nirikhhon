using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lambda
{
    internal class Program
    {
        
        static void Main(string[] args)
        {
            
            //using lambda expration take square 
            Func<int ,int> SquarNo = x => x*2;
            Console.WriteLine(SquarNo(6));

            List<int> list = new List<int> { 1,3,4,5,6,7,8};
            //select each element ant multiplay by 4
            var num_multip = list.Select(x => x*4);
            
            foreach (var x in num_multip)
            {
                Console.Write(x+" ");
            }
            Console.WriteLine();
            //
           // List<int > divby3=list.FindAll(x=>(x%3==0));
            var divby3=num_multip.Where(x=>(x%3==0));
            foreach (var x in divby3)
            {
                Console.Write(x+" ");
            }
            //get first element
            var tem = divby3.First();
            //get last element
            Console.WriteLine(tem);
            tem = divby3.Last();
            Console.WriteLine(tem);
            //create dictionary
            Dictionary<string ,int> map = new Dictionary<string ,int>();
            map.Add("a", 15);
            map.Add("b", 92);
            map.Add("c", 39);
            map.Add("d", 4);
            //order by value
            var newdic = map.OrderBy(x=> x.Value);
            foreach(var x in newdic)
            {
                Console.WriteLine(x+" ");
            }
            //order by key
            newdic = map.OrderBy(x => x.Key);
            foreach (var x in newdic)
            {
                Console.WriteLine(x + " ");
            }



        }
    }
}