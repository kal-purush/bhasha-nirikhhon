using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Primitives;
using Blazor_Kanban_Crud_UrlAdaptor.Data;
using Newtonsoft.Json;
using Syncfusion.Blazor;
using Syncfusion.Blazor.Data;
using System.Collections;

namespace Blazor_Kanban_Crud_UrlAdaptor.Controllers
{
    [ApiController]
    public class DefaultController : ControllerBase
    {
        OrderDataAccessLayer db = new OrderDataAccessLayer();
        //    OrderContext db = new OrderContext();
        // GET: api/Default
        [HttpPost]
        [Route("api/[controller]")]
        public object Post([FromBody] DataManagerRequest dm)
        {
            IEnumerable data = db.GetAllOrders();   //call the method to fetch data from db and return to client
            int count = data.Cast<Order>().Count();
            return dm.RequiresCounts ? new DataResult() { Result = data, Count = count } : (object)data;
        }

        [HttpPost]
        [Route("api/Default/Batch")]
        public void Batch([FromBody]CRUDModel<Order> value)
        {
            if (value.Changed.Count > 0)
            {
                foreach (Order rec in value.Changed)
                {
                    db.UpdateOrder(rec);
                }
            }
            if (value.Added.Count > 0)
            {
                foreach (Order rec in value.Added)
                {
                    db.AddOrder(rec);
                }
            }
            if (value.Deleted.Count > 0)
            {
                foreach (Order rec in value.Deleted)
                {
                    db.DeleteOrder(rec);
                }
            }
        }

        public class CRUDModel<T> where T : class
        {

            [JsonProperty("action")]
            public string Action { get; set; }
            [JsonProperty("table")]
            public string Table { get; set; }
            [JsonProperty("keyColumn")]
            public string KeyColumn { get; set; }
            [JsonProperty("key")]
            public object Key { get; set; }
            [JsonProperty("value")]
            public T Value { get; set; }
            [JsonProperty("added")]
            public List<T> Added { get; set; }
            [JsonProperty("changed")]
            public List<T> Changed { get; set; }
            [JsonProperty("deleted")]
            public List<T> Deleted { get; set; }
            [JsonProperty("params")]
            public IDictionary<string, object> Params { get; set; }
        }
    }
}