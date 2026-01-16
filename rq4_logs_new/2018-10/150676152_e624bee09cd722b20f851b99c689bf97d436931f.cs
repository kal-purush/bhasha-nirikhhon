using System;
using System.Collections.Generic;
using System.Text;

namespace OpenDataImport.Models
{
    public class OpenData
    {




        public int id { get; set; }
        public string 資料集名稱 { get; set; }
        public string 主要欄位說明 { get; set; }
        public string 服務分類 { get; set; }



    }
}
﻿using OpenDataImport.Models;
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
﻿using Models;
using OpenDataImport.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace YC.Repository
{
    public class OpenDataRepository
    {
        public string ConnectionString
        {
            get
            {
                return "";//@"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename=" + YC.Shared.Utils.GetDataPath() + @"OpenData.mdf;Integrated Security=True";
                //return @"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename="+ Directory.GetCurrentDirectory() + @"\App_Data\nodeDB.mdf;Integrated Security=True";
            }
            set => throw new NotImplementedException();
        }
        public void Create(OpenData item)
        {
            var newItem = item;
            var connection = new System.Data.SqlClient.SqlConnection(ConnectionString);
            connection.Open();


            var command = new System.Data.SqlClient.SqlCommand("", connection);
            //command.CommandText = string.Format(@"
            //INSERT INTO OpenData(ID, 資料集名稱, 服務分類, 資料集描述, DisplaySqe)
            //VALUES              ('{0}',N'{1}',N'{2}',N'{3}','{4}')
            //", newItem.ID, newItem.資料集名稱, newItem.服務分類, newItem.資料集描述, newItem.ID);

            command.ExecuteNonQuery();


            connection.Close();
        }



//        public object Update(object item)
//        {
//            var updateItem = item as YC.Models.OpenData;
//            var connection = new System.Data.SqlClient.SqlConnection(ConnectionString);
//            connection.Open();


//            var command = new System.Data.SqlClient.SqlCommand("", connection);
//            command.CommandText = string.Format(@"
//UPDATE [OpenData]
//   SET 
//      [資料集名稱] = N'{0}'
//      ,[服務分類] = N'{1}'
//      ,[資料集描述] = N'{2}'
//      ,[DisplaySqe] = N'{3}'
// WHERE ID=N'{4}'
//            ", updateItem.服務分類, updateItem.資料集名稱, updateItem.資料集描述, updateItem.DisplaySqe, updateItem.ID);

//            command.ExecuteNonQuery();

            
//            connection.Close();
//            return item;
//        }

//        public void Delete(string ID)
//        {
//            var connection = new System.Data.SqlClient.SqlConnection(ConnectionString);
//            connection.Open();


//            var command = new System.Data.SqlClient.SqlCommand("", connection);
//            command.CommandText = string.Format(@"
//DELETE FROM [OpenData]
// WHERE ID=N'{0}'
//            ", ID);

//            command.ExecuteNonQuery();


//            connection.Close();
//        }
    }
}
﻿using System;
using System.Collections.Generic;
using System.Text;

namespace OpenDataImport.Service
{
    public class ImportService
    {



    }
}