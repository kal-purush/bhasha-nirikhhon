using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using StaySteady.Mobile.Droid.Utility;
using StaySteady.Mobile.Utility;
using Xamarin.Forms;

[assembly: Dependency(typeof(SQLite_Droid))]
namespace StaySteady.Mobile.Droid.Utility
{
    public class SQLite_Droid : ISQLite
    {
        public SQLite_Droid() { }
        public SQLite.SQLiteConnection GetConnection()
        {
            var sqliteFilename = "TodoSQLite.db3";
            string documentsPath = System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal); // Documents folder
            var path = Path.Combine(documentsPath, sqliteFilename);
            // Create the connection
            var conn = new SQLite.SQLiteConnection(path);
            // Return the database connection
            return conn;
        }
    }
}