using TheShop.MVVM.Data;
using TheShop.Model;
using System.Collections.ObjectModel;

namespace TheShop.MVVM.ViewModel
{
	public class ProductViewModel : ViewModelBAse
	{
		private IProductDataService _productDataService;
		private Product _selectedProduct;

		public ProductViewModel(IProductDataService productDataService)
		{
			Products = new ObservableCollection<Product>();
			_productDataService = productDataService;
		}

		public void Load()
		{
			var products = _productDataService.GetAll();
			Products.Clear();
			foreach (var product in products)
			{
				Products.Add(product);
			}
		}

		public ObservableCollection<Product> Products { get; set; }

		public Product SelectedProduct {
			get { return _selectedProduct; }
			set { _selectedProduct = value;
				OnPropertyChanged();
			}
		}

	}
}