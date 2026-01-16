using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using SQLite;
using Android.Util;
using App1.Resources.Model;

namespace UGYM.Resources.DataHelperSQLite
{
    public class DataBase
    {
        string folder = System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal);

        public bool createDataBase()
        {
            try
            {
                using (var connection = new SQLiteConnection(System.IO.Path.Combine(folder, "UGYM.db")))
                {
                    connection.CreateTable<Rutina>();
                    return true;
                }
            }
            catch (SQLiteException ex)
            {
                Log.Info("SQLite Exception", ex.Message);
                return false;
            }
        }

        public bool InsertTableRutina(Rutina rutina)
        {
            try
            {
                using (var connection = new SQLiteConnection(System.IO.Path.Combine(folder, "UGYM.db")))
                {
                    connection.Insert(rutina);
                    return true;
                }
            }
            catch (SQLiteException ex)
            {
                Log.Info("SQLite Exception", ex.Message);
                return false;
            }
        }

        public List<Rutina> SelectTableRutina()
        {
            try
            {
                using (var connection = new SQLiteConnection(System.IO.Path.Combine(folder, "UGYM.db")))
                {
                    return connection.Table<Rutina>().ToList();
                }
            }
            catch (SQLiteException ex)
            {
                Log.Info("SQLite Exception", ex.Message);
                return null;
            }
        }

        public bool SelectQueryTableRutina(int Id)
        {
            try
            {
                using (var connection = new SQLiteConnection(System.IO.Path.Combine(folder, "UGYM.db")))
                {
                    connection.Query<Rutina>("SELECT * FROM Rutina Where Id =?", Id);
                    return true;
                }
            }
            catch (SQLiteException ex)
            {
                Log.Info("SQLite Exception", ex.Message);
                return false;
            }
        }


    }
}