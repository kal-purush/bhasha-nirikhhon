using System;
namespace SimplyLinkedList
{
    public class Program
    {
        static void Main()
        {
            LinkedList<string> _carNameList = new LinkedList<string>();
            //add items
            _carNameList.AddUnit("Opel");
            _carNameList.AddUnit("WV");
            _carNameList.AddUnit("Subaru");
            _carNameList.AddUnit("Toyota");
            _carNameList.AddUnit("Volvo");
            _carNameList.AddUnit("Nissan");

            foreach (var item in _carNameList)
            {
                Console.WriteLine(item);
            }
            Console.WriteLine("====================");
            //delete item
            _carNameList.DeleteUnit("Opel");
			foreach (var item in _carNameList)
			{
				Console.WriteLine(item);
			}
			Console.WriteLine("====================");
            //add first item
            _carNameList.AppendFirstUnit("Vaz");

			foreach (var item in _carNameList)
			{
				Console.WriteLine(item);
			}
            Console.ReadLine();
        }
    }
}