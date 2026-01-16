using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ObserverDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            var registry = new Registry();
            var warehouseManager = new WarehouseManager();
            var warehouse = new Warehouse();
            warehouse.subscribe(registry);
            warehouse.subscribe(warehouseManager);

            warehouse.addWare("Product A", 100);
            warehouse.addWare("Product B", 50);

            warehouse.removeWare("Product A", 100);
            warehouse.removeWare("Product B", 10);

            warehouse.Describe();
            Console.Read();
        }
    }
}