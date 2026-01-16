using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Economic
{
	public class PriceManager
	{
		private const string pricesSheetName = "prices";

		public int GetItemPrice(string itemClassName)
		{
			int price = 0;

			if(itemClassName == null || itemClassName == string.Empty)
			{
				return 1;
			}

			List<object> itemInfo = GSWorker.Instance.ReadDataFromSheet(pricesSheetName, "A:B")
				.Single(item => item.ElementAt(0).ToString() == itemClassName);

			try
			{
				price = Convert.ToInt32(itemInfo.ElementAt(1));
			}
			catch
			{
				return 2;
			}

			return price;
		}

		public List<object> GetItemInfo(string itemClassName)
		{
			List<object> itemInfo = GSWorker.Instance.ReadDataFromSheet(pricesSheetName, "A:B")
				.Single(item => item.ElementAt(0).ToString() == itemClassName);

			return itemInfo;
		}
	}
}