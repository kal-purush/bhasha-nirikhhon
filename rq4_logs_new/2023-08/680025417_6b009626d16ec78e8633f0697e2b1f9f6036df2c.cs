//using Microsoft.AspNetCore.Mvc;
//using Microsoft.AspNetCore.Mvc.RazorPages;
//using System.Text.Json.Nodes;
//using System.Xml.Schema;

//namespace ShopOnline.Web.CheckOut
//{
//    public class CheckOutModel : PageModel
//    {

//        public string PaypalClientId { get; set; } = "";
//        public string PaypalSecret { get; set; } = "";

//        public string PaypalUrl { get; set; } = "";

//        public CheckOutModel(IConfiguration configuration)
//        {
//            PaypalClientId = configuration["PayPalSettings:ClientId"];
//            PaypalSecret = configuration["PaypalSettings:Secret"];
//            PaypalUrl = configuration["PaypalSettings:Url"];
//        }
//        public void OnGet()
//        {
//            DeleveryAddress = TempData["DeliveryAddress"]?.ToString() ?? "";
//            Total = TempData["Total"]?.ToString() ?? "";
//            ProductIdentifiers = TempData["ProductIdentifiers"]?.ToString() ?? "";

//            TempData.Keep();

//            if(DeleveryAddress == ""  || Total == "" || ProductIdentifiers == "")
//            {
//                Response.Redirect("/");
//                return;
//            }
//        }


//        public JsonResult OnPostCreateOrder()
//        {
//            var response = new
//            {
//                Id = "",

//            };
//        }

//        public JsonResult OnPostCompleteOrder([FromBody] JsonObject data)
//        {

//        }


//        public JsonResult OnPostCancelOrder([FromBody] JsonObject data)
//        {

//        }


//    }
//}

