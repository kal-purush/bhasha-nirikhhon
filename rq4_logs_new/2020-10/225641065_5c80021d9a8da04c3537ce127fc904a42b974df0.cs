using Restauracja.Model;
using Restauracja.Utilities;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Restauracja.Extensions
{
    public static class ViewModelExtensions
    {
        public static void GetCatchedData(this IWithCacheableData viewModel)
        {
            if (SingleOrder.Instance.Order != null)
                viewModel.Order = SingleOrder.Instance.Order;

            if (SingleOrder.Instance.Order?.Products?.Count > 0)
                viewModel.OrderProducts = new ObservableCollection<ProductPOCO>(SingleOrder.Instance.Order.Products);
            Console.WriteLine($"Got data from cache ({viewModel.GetType().Name})!!!!!!!!!!!!!!");
        }
    }
}