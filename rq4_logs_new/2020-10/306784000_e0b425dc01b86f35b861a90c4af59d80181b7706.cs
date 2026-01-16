using System;
using System.Collections.Generic;
using System.Text;

namespace AbstractFactory
{
    public class Factory
    {
       
        public static IFactory CreateInstance(string type)
        {
            switch (type)
            {
                case "sqlserver":
                    return new SqlServerFactory();
                case "sqllite":
                    return new SqlLiteFactory();
                default:
                    break;
            }
            return null;
        }
    }
}