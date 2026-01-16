using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Resources;
using System.Windows.Forms;

namespace legendary_crafter
{
    internal class Api
    {
        public int Id
        {
            get;
            set;
        }

        public Dictionary<string, int> Buys
        {
            get;
            set;
        }

        public Dictionary<string, int> Sells
        {
            get;
            set;
        }

        public int GetSellPrice(int id)
        {
            var url = "https://api.guildwars2.com/v2/commerce/prices/" + id;
            var sellprice = _download_serialized_json_data<Api>(url);
            var sell = sellprice.Sells["unit_price"];
            return sell;
        }

        private static T _download_serialized_json_data<T>(string url) where T : new()
        {
            using (var w = new WebClient())
            {
                var jsonData = string.Empty;
                // attempt to download JSON data as a string
                try
                {
                    jsonData = w.DownloadString(url);
                }
                catch (Exception)
                {
                    // ignored
                }
                // if string with JSON data is not empty, deserialize it to class and return its instance
                return !string.IsNullOrEmpty(jsonData) ? JsonConvert.DeserializeObject<T>(jsonData) : new T();
            }
        }
    }
}