using System;
using System.IO;
using SQLite;

using NotifyMe.Droid;
using NotifyMe.ServiceInterfaces;


[assembly: Xamarin.Forms.Dependency(typeof(SqliteDbConnection_Android))]

namespace NotifyMe.Droid
{
    class SqliteDbConnection_Android : ISqliteConnection
    {
        public SQLiteConnection GetConnection()
        {
            var dbName = "NotifyMe.db3";
            var documentsPath = Environment.GetFolderPath(Environment.SpecialFolder.Personal);
            var path = Path.Combine(documentsPath, dbName);

            return new SQLiteConnection(path);
        }
    }
}