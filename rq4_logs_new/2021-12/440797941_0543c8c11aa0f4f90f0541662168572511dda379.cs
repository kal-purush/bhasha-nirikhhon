using Microsoft.AspNetCore.Mvc;
using MusicApi.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.SqlClient;
using System.Data.Common;
using System.Data;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace MusicApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MusicController : ControllerBase
    {
        // GET: api/<MusicController>
        [HttpGet]
        public object Get()
        {
            var result = new Result();

            var sqlStringBuilder = new SqlConnectionStringBuilder();
            sqlStringBuilder["Server"] = "DESKTOP-RL88JED\\ROMCOCA";
            sqlStringBuilder["Database"] = "music";
            sqlStringBuilder["Trusted_Connection"] = "True";
            
            //string connection = "Server=DESKTOP-RL88JED\\ROMCOCA; Database=music; Trusted_Connection=True;";
            string connection = sqlStringBuilder.ToString();
            using var sqlConnection = new SqlConnection(connection);

            sqlConnection.Open();
            using DbCommand cmd = new SqlCommand();
            cmd.Connection = sqlConnection;
            cmd.CommandText = "SELECT * FROM musics";

            using var sqlDataReader = cmd.ExecuteReader();

            if(sqlDataReader.HasRows)
            {
                List<Music> list = new List<Music>();
                while (sqlDataReader.Read())
                {
                    //Console.WriteLine($"{sqlDataReader["id"]} - {sqlDataReader["song"]}");
                    list.Add(new Music()
                    {
                        Id = (int)sqlDataReader["id"],
                        Song = (string)sqlDataReader["song"],
                        Link = (string)sqlDataReader["link"],
                        Singer = (string)sqlDataReader["singer"],
                        Author = (string)sqlDataReader["author"],
                    });
                }
                result.Data = list;
                result.Message = "Thành công";
                result.TotalResult = list.Count;
 
            }
           
            sqlConnection.Close();
            //return Data.MusicData.Values.ToList();
            //return Data.MusicData.Select(music => music.Value).ToList();
            return result;
        }

        // GET api/<MusicController>/5
        [HttpGet("{id}")]
        public object Get(int id)
        {
            var result = new Result();
            var sqlStringBuilder = new SqlConnectionStringBuilder();
            sqlStringBuilder["Server"] = "DESKTOP-RL88JED\\ROMCOCA";
            sqlStringBuilder["Database"] = "music";
            sqlStringBuilder["Trusted_Connection"] = "True";

            string connection = sqlStringBuilder.ToString();
            using var sqlConnection = new SqlConnection(connection);

            sqlConnection.Open();
            using DbCommand cmd = new SqlCommand();
            cmd.Connection = sqlConnection;
            cmd.CommandText = "SELECT * FROM musics WHERE id = @MusicId";
            var musicId = new SqlParameter("@MusicId", id);
            cmd.Parameters.Add(musicId);

            using var sqlDataReader = cmd.ExecuteReader(); // Lấy nhiều dòng dữ liệu
            //var sqlDataReader = cmd.ExecuteScalar(); //Trả về dữ liệu hàng 1 cột 1 (Count)
            //var sqlDataReader = cmd.ExecuteNonQuery(); //Dùng để thêm, sửa xoá  

            //var dataTable = new DataTable();
            //dataTable.Load(sqlDataReader);
            if (sqlDataReader.HasRows)
            {
                result.Message = "Thành công";
                sqlDataReader.Read();
                result.Data = new List<Music>()
                {
                    new Music()
                    {
                        Id = sqlDataReader.GetInt32(0),
                        Song = (string)sqlDataReader["song"],
                        Link = (string)sqlDataReader["link"],
                        Singer = (string)sqlDataReader["singer"],
                        Author = (string)sqlDataReader["author"],
                    }
                };
                result.TotalResult = result.Data.ToList().Count;
            }
            else
            {
                result.Message = "Không có dữ liệu.";
            }
            sqlConnection.Close();
            return result;
        }

        // POST api/<MusicController>
        [HttpPost]
        public object Post([FromBody] Music music)
        {
            var result = new Result();

            if(music == null)
            {
                result.Status = 400;
                result.Message = "Hãy nhập đủ các trường";
                return result;
            }

            if (music.CheckNull())
            {
                return music.GetResult();
            }

            var sqlStringBuilder = new SqlConnectionStringBuilder();
            sqlStringBuilder["Server"] = "DESKTOP-RL88JED\\ROMCOCA";
            sqlStringBuilder["Database"] = "music";
            sqlStringBuilder["Trusted_Connection"] = "True";

            string connection = sqlStringBuilder.ToString();
            using var sqlConnection = new SqlConnection(connection);

            sqlConnection.Open();
            using DbCommand cmd = new SqlCommand();
            cmd.Connection = sqlConnection;
            cmd.CommandText = "INSERT INTO musics (song, link, singer, author) VALUES (@Song, @Link, @Singer, @Author); SELECT SCOPE_IDENTITY();";
            cmd.Parameters.Add(new SqlParameter("@Song", music.Song));
            cmd.Parameters.Add(new SqlParameter("@Link", music.Link));
            cmd.Parameters.Add(new SqlParameter("@Singer", music.Singer));
            cmd.Parameters.Add(new SqlParameter("@Author", music.Author));


            //using var sqlDataReader = cmd.ExecuteReader(); // Lấy nhiều dòng dữ liệu
            var sqlDataReader = cmd.ExecuteScalar(); //Trả về dữ liệu hàng 1 cột 1 (Count)
            //var sqlDataReader = cmd.ExecuteNonQuery(); //Dùng để thêm, sửa xoá  

            //var dataTable = new DataTable();
            //dataTable.Load(sqlDataReader);
            int id = Int32.Parse(sqlDataReader.ToString());
            if (id != 0)
            {
                music.Id = id;
                result.Message = "Thành công";
                result.Data = new List<Music>()
                {
                    music
                };
                result.TotalResult = result.Data.ToList().Count;
            }
            else
            {
                result.Message = "Thêm mới thất bại.";
            }
            sqlConnection.Close();
            return result;
        }

        // PUT api/<MusicController>/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] Music music)
        {

        }

        // DELETE api/<MusicController>/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}