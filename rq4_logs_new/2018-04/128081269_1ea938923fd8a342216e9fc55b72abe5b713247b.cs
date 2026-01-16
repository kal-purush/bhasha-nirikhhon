using System;
using System.Collections.Generic;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Data;

public partial class _Default : System.Web.UI.Page {
	private Dictionary<int, string> products = new Dictionary<int, string>();
	protected void Page_Load(object sender, EventArgs e) {
		if(!IsPostBack)
			GetProductsByCategory();
	}

	private void GetProductsByCategory() {
		DataView p;
		p = (DataView)dsProducts.Select(DataSourceSelectArguments.Empty);

		foreach (DataRowView r in p) {
			int categoryId = (int)r["CategoryID"];
			if (products.ContainsKey(categoryId)) {
				products[categoryId] = String.Format("{0}#{1};{2}", products[categoryId], r[0], r[1]);
			}
			else
				products[categoryId] = String.Format("{0};{1}", r[0], r[1]);
		}

		foreach (int id in products.Keys) {
			hField.Add(id.ToString(), products[id]);
		}
	}
}