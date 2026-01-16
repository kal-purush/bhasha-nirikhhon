using Dapper;
using ReportGenerator.DataBase.Models;
using ReportGenerator.Services;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;


namespace ReportGenerator.DataBase.Controls
{
    /// <summary>
    /// Класс управления Отделом (работа с БД)
    /// </summary>
    public class DepartamentControl
    {
        public static string GetNameById(int id)
        {
            
            Departament departament = null;
            DbConnection db = new DbConnection();
            SqlConnection conn = db.GetConnection();

            using (conn)
            {
                departament = conn.Query<Departament>("SELECT name FROM departaments WHERE id = @id", new { id }).FirstOrDefault();
            }

            return departament.name;

        }

        public static List<string> GetAllNameDepartaments()
        {

            List<Departament> departaments = new List<Departament>();
            DbConnection db = new DbConnection();
            SqlConnection conn = db.GetConnection();

            using (conn)
            {
                departaments = conn.Query<Departament>("Select name From departaments").ToList();
            }

            List<string> departamentNames = new List<string>();
            foreach (Departament departament in departaments)
            {
                departamentNames.Add(departament.name);
            }

            return departamentNames;
        }

        public static int GetIddByName(string name)
        {

            Departament departament = new Departament();
            DbConnection db = new DbConnection();
            SqlConnection conn = db.GetConnection();

            using (conn)
            {
                departament = conn.Query<Departament>("SELECT id FROM departaments WHERE name = @name", new { name }).FirstOrDefault();
            }

            return departament.id;

        }

        public static List<Departament> GetAllDepartamentsList()
        {

            List<Departament> departaments = new List<Departament>();
            DbConnection db = new DbConnection();
            SqlConnection conn = db.GetConnection();

            using (conn)
            {
                departaments = conn.Query<Departament>("Select * From departaments").ToList();
            }

            return departaments;
        }

        /// <summary>
        /// Добавить новый отдел
        /// </summary>
        /// <param name="departament"></param>
        public static void InsertNewDepartament(Departament departament)
        {
            DbConnection db = new DbConnection();
            SqlConnection conn = db.GetConnection();

            using (conn)
            {                
                string insertQuery = "INSERT INTO departaments (name, shortName, description) VALUES (@name, @shortName, @description)";
                var result = conn.Execute(insertQuery, departament);
            }
        }

        /// <summary>
        /// Обновить текущий отдел
        /// </summary>
        /// <param name="departament"></param>
        public static void UpdateCurrentDepartament(Departament departament)
        {
            DbConnection db = new DbConnection();
            SqlConnection conn = db.GetConnection();

            using (conn)
            {
                string updatetQuery = "UPDATE departaments SET name = @name, shortName = @shortName, description = @description WHERE id = @id";
                var result = conn.Execute(updatetQuery, departament);
            }
        }

        /// <summary>
        /// Удалить текущий отдел
        /// </summary>
        /// <param name="id"></param>
        public static void DeleteCurrentDepartament(int id)
        {
            DbConnection db = new DbConnection();
            SqlConnection conn = db.GetConnection();

            using (conn)
            {
                string deleteQuery = "DELETE FROM departaments WHERE id = @id";
                var result = conn.Execute(deleteQuery, new
                {
                    id
                });
            }
        }
    }
}