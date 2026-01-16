using QTFK.Attributes;
using QTFK.Models;
using QTFK.Services;
using System.Reflection;

namespace QTFK.Extensions.DBIO.EngineAttribute
{
    public static class DBAttributeExtension
    {
        public static string getDBEngine<T>(this T item) where T : class, IDBQuery, IQueryFactory, IDBIO
        {
            DBAttribute dbAttribute;

            Asserts.isSomething(item, $"Parameter '{nameof(item)}' cannot be null.");

            dbAttribute = item
                .GetType()
                .GetCustomAttribute<DBAttribute>();

            Asserts.isSomething(dbAttribute, $"Class '{typeof(T).FullName}' has no DBAttribute.");
            return dbAttribute.Engine;
        }
    }
}