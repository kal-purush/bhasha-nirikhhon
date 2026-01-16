using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Windows.Web.Http;
using Newtonsoft.Json;
using Windows.Storage.Streams;
namespace T2008M_UWP.Services
{
    class OrderService
    {
        public async Task<Models.CreateOrder>CreateOrder(){
            CartService cs = new CartService();
            var items = cs.GetCart();
            // cach 2: items co the truyen trong tham so cuar function
            Adapters.FoodGroup fg = Adapters.FoodGroup.GetInstance();
            HttpClient httpClient = new HttpClient();
            Uri uri = new Uri(fg.ApiCreateOrder);
            HttpStringContent content = new HttpStringContent(
                    "items:"+ JsonConvert.SerializeObject(items),
                    UnicodeEncoding.Utf8,
                    "application/json"
                );
            HttpResponseMessage msg = await httpClient.PostAsync(uri, content);
            msg.EnsureSuccessStatusCode();
            var rsBody = await msg.Content.ReadAsStringAsync();
            Models.CreateOrder createOrder = JsonConvert.DeserializeObject<Models.CreateOrder>(rsBody);
            return createOrder;
            // sau khi nhan duoc order id -> luu vao 1 table trong SQLite de lam trang danh sach don hang
        }

    }
}