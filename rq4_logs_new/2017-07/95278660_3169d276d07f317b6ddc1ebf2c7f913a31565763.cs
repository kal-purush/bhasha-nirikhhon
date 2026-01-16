using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using DataParser.HelperClasses;
using MySql.Data.MySqlClient;

namespace DataParser
{
    class DbConnector
    {
        public MySqlConnection Connection { get; }

        public DbConnector(string user, string pass, string server, string dbName)
        {
            Connection = new MySqlConnection($"Server={server}; database={dbName}; UID={user}; password={pass};");
            Connection.Open();
        }


        public IEnumerable<ProductCategoryObject> GetProductCategoryObjects(
            string query,
            Dictionary<string, Func<MySqlDataReader, string>> singlePropertiesProductFunc,
            Dictionary<string, Func<MySqlDataReader, string>> singlePropertiesCategoryFunc,
            Dictionary<string, Func<MySqlDataReader, string[]>> pluralPropertiesProductFunc = null,
            Dictionary<string, Func<MySqlDataReader, string[]>> pluralPropertiesCategoryFunc = null)
        {
            pluralPropertiesCategoryFunc = pluralPropertiesProductFunc ??
                                           new Dictionary<string, Func<MySqlDataReader, string[]>>();
            pluralPropertiesProductFunc = pluralPropertiesProductFunc ??
                                          new Dictionary<string, Func<MySqlDataReader, string[]>>();
            using (var command = new MySqlCommand(query, Connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        yield return new ProductCategoryObject(
                            singleProperties: singlePropertiesCategoryFunc.ToDictionary(x => x.Key,
                                x => x.Value(reader)),
                            pluralProperties: pluralPropertiesCategoryFunc.ToDictionary(x => x.Key,
                                x => x.Value(reader)));
                        yield return new ProductCategoryObject(
                            singleProperties:
                            singlePropertiesProductFunc.ToDictionary(x => x.Key, x => x.Value(reader)),
                            pluralProperties: pluralPropertiesProductFunc.ToDictionary(x => x.Key,
                                x => x.Value(reader)));
                    }
                }
            }
        }
    }
}