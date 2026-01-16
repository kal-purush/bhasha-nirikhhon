using OpenDataImport.Models;
using System;
using System.Collections.Generic;
using System.IO;
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
        public void Configure()
        {
            string baseDir = Directory.GetCurrentDirectory();

            AppDomain.CurrentDomain.SetData("DataDirectory", System.IO.Path.Combine(baseDir, "App_Data"));
        }
        static List<OpenData> findOpenData()
        {
            List<OpenData> result = new List<OpenData>();

            string baseDir = Directory.GetCurrentDirectory();


            var xml = XElement.Load(System.IO.Path.Combine(baseDir, "App_Data/datagovtw_dataset_20181005.xml"));


            //XNamespace gml = @"http://www.opengis.net/gml/3.2";
            //XNamespace twed = @"http://twed.wra.gov.tw/twedml/opendata";
            var nodes = xml.Descendants("node").ToList();


            result = nodes
                .Where(x => !x.IsEmpty).ToList()
                .Select(node =>
                {
                    OpenData item = new OpenData();
                    item.id =int.Parse(getValue(node, "id"));
                    item.資料集名稱 = getValue(node, "資料集名稱");
                    item.服務分類 = getValue(node, "服務分類");
                    item.主要欄位說明 = getValue(node, "主要欄位說明");
                    return item;
                }).ToList();
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