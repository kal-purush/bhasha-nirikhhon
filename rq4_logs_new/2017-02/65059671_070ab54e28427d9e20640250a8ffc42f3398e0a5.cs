using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Mono.Data.Sqlite;
using MySql.Data.MySqlClient;
using TShockAPI;
using TShockAPI.DB;

namespace ClassWars
{
    public class ClassDatabase
    {
        private IDbConnection _db;

        public ClassDatabase(IDbConnection db)
        {
            _db = db;
            var sqlCreator = new SqlTableCreator(_db,
                _db.GetSqlType() == SqlType.Sqlite
                    ? (IQueryBuilder)new SqliteQueryCreator()
                    : new MysqlQueryCreator());
            var table = new SqlTable("Classes",
                new SqlColumn("Name", MySqlDbType.String),
                new SqlColumn("Category", MySqlDbType.String),
                new SqlColumn("Description", MySqlDbType.String),
                new SqlColumn("Buffs", MySqlDbType.String),
                new SqlColumn("Itembuffs", MySqlDbType.String),
                new SqlColumn("Ammo", MySqlDbType.String),
                new SqlColumn("Inventory", MySqlDbType.String),
                new SqlColumn("MaxHealth", MySqlDbType.Int32),
                new SqlColumn("MaxMana", MySqlDbType.Int32),
                new SqlColumn("ExtraSlot", MySqlDbType.Int32));
            sqlCreator.EnsureTableStructure(table);
        }

        public static ClassDatabase InitDb(string name)
        {
            IDbConnection db;
            if (TShock.Config.StorageType.ToLower() == "sqlite")
                db =
                    new SqliteConnection(string.Format("uri=file://{0},Version=3",
                        Path.Combine(TShock.SavePath, name + ".sqlite")));
            else if (TShock.Config.StorageType.ToLower() == "mysql")
            {
                try
                {
                    var host = TShock.Config.MySqlHost.Split(':');
                    db = new MySqlConnection
                    {
                        ConnectionString = string.Format("Server={0}; Port={1}; Database={2}; Uid={3}; Pwd={4}",
                            host[0],
                            host.Length == 1 ? "3306" : host[1],
                            TShock.Config.MySqlDbName,
                            TShock.Config.MySqlUsername,
                            TShock.Config.MySqlPassword
                            )
                    };
                }
                catch (MySqlException x)
                {
                    TShock.Log.Error(x.ToString());
                    throw new Exception("MySQL not setup correctly.");
                }
            }
            else
                throw new Exception("Invalid storage type.");
            var statDatabase = new ClassDatabase(db);
            return statDatabase;
        }

        public QueryResult QueryReader(string query, params object[] args)
        {
            return _db.QueryReader(query, args);
        }

        public int Query(string query, params object[] args)
        {
            return _db.Query(query, args);
        }

        public int AddClass(Classvar x)
        {
            StorageFriendlyClassvar copy = Translations.dataPrep(x);
            Query("INSERT INTO Classes (Name, Category, Description, Buffs, Itembuffs, Ammo, Inventory, MaxHealth, MaxMana, ExtraSlot) VALUES (@0, @1, @2, @3, @4, @5, @6, @7, @8, @9)",
                copy.name, copy.category, copy.description, copy.buffs, copy.itembuffs, copy.ammo, copy.inventory, copy.maxHealth, copy.maxMana, copy.extraSlot);

            using (var reader = QueryReader("SELECT max(ID) FROM Classes"))
            {
                if (reader.Read())
                {
                    var id = reader.Get<int>("max(ID)");
                    return 1;
                }
            }

            return -1;
        }

        public void DeleteClass(string name)
        {
            Query("DELETE FROM Classes WHERE Name = @0", name);
        }

        public void UpdateClass(Classvar x)
        {
            StorageFriendlyClassvar update = Translations.dataPrep(x);
            var query =
                string.Format(
                    "UPDATE Classes SET Category = {0}, Description = {1}, Buffs = {2}, Itembuffs = {3}, Ammo = {4}, Inventory = {5}, MaxHealth = {6}, MaxMana = {7}, ExtraSlot = {8} WHERE Name = @0",
                    update.category, update.description, update.buffs, update.itembuffs, update.ammo, update.inventory, update.maxHealth, update.maxMana, update.extraSlot);

            Query(query, update.name);
        }

        public void LoadClasses(ref List<Classvar> list)
        {
            using (var reader = QueryReader("SELECT * FROM Classes"))
            {
                while (reader.Read())
                {
                    var name = reader.Get<string>("Name");
                    var category = reader.Get<string>("Category");
                    var description = reader.Get<string>("Description");
                    var buffs = reader.Get<string>("Buffs");
                    var itembuffs = reader.Get<string>("Itembuffs");
                    var ammo = reader.Get<string>("Ammo");
                    var inventory = reader.Get<string>("Inventory");
                    var maxHealth = reader.Get<int>("MaxHealth");
                    var maxMana = reader.Get<int>("MaxMana");
                    var extraSlot = reader.Get<int>("ExtraSlot");

                    var temp = new StorageFriendlyClassvar(name, category, description, buffs, itembuffs, ammo, inventory, maxHealth, maxMana, extraSlot);
                    Classvar x = (Translations.dataUnPrep(temp));
                    list.Add(x);
                }
            }
        }
    }
}