using heron_najam.ViewModel;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace heron_najam.DAL
{
    public class AutomobilDAL
    {
        private readonly String ConnString;

        public AutomobilDAL()
        {
            ConnString = ConfigurationManager.ConnectionStrings["autoConnectionString"].ConnectionString;
        }

        /// <summary>
        /// Dodaj automobil u bazu podataka
        /// </summary>
        /// <param name="Naziv"></param>
        /// <returns></returns>
        public async Task KreirajAuto(AutomobilVM Automobil)
        {
            //spoji se na bazu
            using (var conn = new SqlConnection(ConnString))
            {
                // otvori konekciju
                await conn.OpenAsync();

                // kreiraj naredbu na konekciji
                using (var cmd = new SqlCommand("INSERT INTO automobili (naziv) VALUES (@inputNaziv)", conn))
                {
                    // dodaj vrijednost parametra @Naziv
                    cmd.Parameters.AddWithValue("@inputNaziv", Automobil.Naziv);

                    // izvrši query
                    await cmd.ExecuteNonQueryAsync();

                }
            }
        }

        /* Izbriši automobil */
        public async Task IzbrisiAuto(int Id)
        {
            //spoji se na bazu
            using (var conn = new SqlConnection(ConnString))
            {
                // otvori konekciju
                await conn.OpenAsync();

                using (var cmd = new SqlCommand("DELETE FROM automobili WHERE Id = @Id", conn))
                {
                    cmd.Parameters.AddWithValue("@Id", Id);

                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        /* Izlistaj automobile */
        public async Task<List<AutomobilVM>> GetAutomobili()
        {
            //spoji se na bazu
            using (var conn = new SqlConnection(ConnString))
            {
                // otvori konekciju
                await conn.OpenAsync();

                using (var comm = new SqlCommand("SELECT * FROM Automobili", conn))
                {
                    using (var reader = await comm.ExecuteReaderAsync())
                    {
                        return reader.MapToList<AutomobilVM>();
                    }
                }
            }
        }
    }
}