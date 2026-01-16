using Model;
using o2o.Utils;
using Service;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;

namespace o2o.Controller
{
    /// <summary>
    /// Summary description for SupplyManagementController
    /// </summary>
    public class SupplyManagementController : IHttpHandler
    {

        SupplyService supplyService = new SupplyService();
        public void ProcessRequest(HttpContext context)
        {
            context.Response.ContentType = "text/plain";
            String mapping = context.Request["action"];
            switch (mapping)
            {
                case "supplyListManagement": supplyListManagement(context); break;
                case "updateSupply": updateSupply(context); break;
                default: break;
            }
        }
        public void updateSupply(HttpContext context)
        {
            context.Response.ContentType = "text/plain";
            Supply supply = new Supply();
            supply.Id = Convert.ToInt32(context.Request.Form["id"].ToString());
            supply.Priority = Convert.ToInt32(context.Request.Form["priority"].ToString());
            supply.SupplyStatus = Convert.ToInt32(context.Request.Form["status"].ToString());
            Boolean flag = supplyService.updateSupplyByManager(supply);
            Dictionary<String, Object> dictionary = new Dictionary<string, object>();
            if (flag)
            {
                dictionary.Add("success", "true");
            }
            else
            {
                dictionary.Add("success", "false");
            }
            StringBuilder sb = JsonUtil.toJson(dictionary);
            context.Response.Write(sb.ToString());
        }
        public void supplyListManagement(HttpContext context)
        {
            context.Response.ContentType = "text/plain";
            List<Supply> list = supplyService.getAllSupplyList();
            StringBuilder jsonString = new StringBuilder();
            Dictionary<String, Object> dictionary = new Dictionary<string, object>();
            //jsonString.Append("\"supply\":");
            jsonString.Append("[");
            int i = 1;
            foreach (Supply supply in list)
            {
                dictionary.Add("Id", supply.Id);
                dictionary.Add("supplyName", supply.SupplyName);
                dictionary.Add("supplyDesc", supply.SupplyDesc);
                dictionary.Add("categoryId", supply.SupplyCategory.Id);
                dictionary.Add("priority", supply.Priority);
                dictionary.Add("userId", supply.User.Id);
                dictionary.Add("nickName", supply.User.NickName);
                dictionary.Add("teleNumber", supply.User.TeleNumber);
                dictionary.Add("createTime", supply.CreateTime);
                dictionary.Add("modifyTime", supply.ModifyTime);
                dictionary.Add("supplyStatus", supply.SupplyStatus);
                dictionary.Add("success", "true");
                jsonString.Append(JsonUtil.toJson(dictionary));
                if (i < list.Count)
                {
                    jsonString.Append(",");
                }
                i++;
                dictionary.Clear();
            }
            jsonString.Append("]");
            context.Response.Write(jsonString.ToString());

        }

        public bool IsReusable
        {
            get
            {
                return false;
            }
        }
    }
}