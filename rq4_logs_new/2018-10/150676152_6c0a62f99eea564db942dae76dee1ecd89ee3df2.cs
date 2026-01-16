using OpenDataImport.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;

namespace OpenDataImport
{
    class Program
    {
        static void Main(string[] args)
        {
            var nodes=findOpenData();
            showOpenData(nodes);
            Console.ReadKey();

        }
        static List<OpenData> findOpenData()
        {
            List<OpenData> result = new List<OpenData>();



            var xml = XElement.Load(@"D:\Files\Documents\Works\datagovtw_dataset_20181005.xml");


            //XNamespace gml = @"http://www.opengis.net/gml/3.2";
            //XNamespace twed = @"http://twed.wra.gov.tw/twedml/opendata";
            var nodes = xml.Descendants("node").ToList();

            for (var i = 0; i < nodes.Count; i++)
            {
                var node = nodes[i];
                OpenData item = new OpenData();

                item.id = int.Parse(getValue(node, "id"));
                item.資料集名稱=getValue(node, "資料集名稱");
                item.主要欄位說明 = getValue(node, "主要欄位說明");
                item.服務分類 = getValue(node, "服務分類");
                result.Add(item);
            }

            //result = nodes
            //    .Where(x => !x.IsEmpty).ToList()
            //    .Select(node =>
            //    {
            //        OpenData item = new OpenData();
            //        item.ID = getValue(node, "ID");
            //        item.資料集名稱 = getValue(node, "資料集名稱");
            //        item.服務分類 = getValue(node, "服務分類");
            //        item.資料集描述 = getValue(node, "資料集描述");
            //        return item;

            //    }).ToList();
            return result;

        }
        private static string getValue(XElement node, string propertyName)
        {
            return node.Element(propertyName)?.Value?.Trim();

        }


        private static void showOpenData(List<OpenData> nodes)
        {

            Console.WriteLine(string.Format("共收到{0}筆的資料", nodes.Count));
            nodes.GroupBy(node => node.服務分類).ToList()
                .ForEach(group =>
                {

                    var key = group.Key;
                    var groupDatas = group.ToList();
                    var message = $"服務分類:{key},共有{groupDatas.Count()}筆資料";
                    Console.WriteLine(message);
                });


        }
    }

}