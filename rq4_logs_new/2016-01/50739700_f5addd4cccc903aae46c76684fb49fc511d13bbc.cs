using System;
using SQLite.Net;
using System.IO;
using Xamarin.Forms;
using SQLite;

[assembly: Dependency(typeof(LifeStyle.Droid.SQLiteAccess_Droid))]
namespace LifeStyle.Droid
{
	public class SQLiteAccess_Droid : ISQLite
	{
		public SQLiteAccess_Droid()
		{
		}

		public SQLite.Net.SQLiteConnection GetConnection()
		{
			var sqliteFilename = "LifeStyle.db3";
			string documentsPath = System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal);
			var path = Path.Combine(documentsPath, sqliteFilename);
			var plat = new SQLite.Net.Platform.XamarinAndroid.SQLitePlatformAndroid();
			var conn = new SQLite.Net.SQLiteConnection(plat, path);
			return conn;
		}
	}
}