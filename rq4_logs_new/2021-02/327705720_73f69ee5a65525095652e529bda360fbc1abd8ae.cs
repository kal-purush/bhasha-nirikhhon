using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using BRIM;

namespace React.Sample.Webpack.CoreMvc.Controllers
{
	public class InventoryController: Controller
	{
		private const int ITEMS_PER_PAGE = 15;

		private Inventory inventory;
		private const int COMMENTS_PER_PAGE = 3;


		public InventoryController()
		{
			//initialize the inventory
			inventory = new Inventory();
			
		}
		public ActionResult Items(){
			return View(new ItemViewModel{
				Items = this.inventory.ItemList.Take(ITEMS_PER_PAGE).ToList().AsReadOnly()
			});
		}
/*
		public ActionResult Index()
		{
			return View(new IndexViewModel
			{
				Comments = _comments.Take(COMMENTS_PER_PAGE).ToList().AsReadOnly(),
				CommentsPerPage = COMMENTS_PER_PAGE,
				Page = 1
			});
		}
		*/

		public class ItemViewModel
		{
			public IReadOnlyList<Item> Items { get; set; }

		}

/*
		public class IndexViewModel
		{
			public IReadOnlyList<CommentModel> Comments { get; set; }
			public int CommentsPerPage { get; set; }
			public int Page { get; set; }
		}
		*/
	}
}