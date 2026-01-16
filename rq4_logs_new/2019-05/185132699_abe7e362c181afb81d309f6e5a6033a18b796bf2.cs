using System;
using System.Collections.Generic;
using System.Text;
using System.Web;

namespace o2o.Utils
{
    public class JsonUtil
    {
        public static StringBuilder toJson(Dictionary<String, Object> dictionory)
        {
            StringBuilder jsonString = new StringBuilder();
            jsonString.Append("{");
            foreach(KeyValuePair<string,Object> kvp in dictionory)
            {
                jsonString.Append("\"" + kvp.Key + "\":"+"\""+kvp.Value+"\"");
            }
            jsonString.Append("}");
            return jsonString;
        }
    }
}