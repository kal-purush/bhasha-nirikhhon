using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Products
{
    class ConsoleView : IView
    {
        private List<Product> list;
        public ConsoleView(List<Product> list)
        {
            this.list = list;
        }
        public void RenderView()
        {
            Console.WriteLine("ConsoleView says hi and renders some items.");
        
            foreach (var item in list)
            {
                Console.WriteLine(item.ProductName);
            }
        }
    }
}