using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using SomerenModel;

namespace SomerenDAL
{
    public class RoomDao : BaseDao
    {
        public List<Room> GetAllRooms()
        {
            string query = "SELECT RoomID, Building, RoomType, RoomCapacity FROM dbo.ROOM";
            SqlParameter[] sqlParameters = new SqlParameter[0];
            return ReadTables(ExecuteSelectQuery(query, sqlParameters));
        }

        private List<Room> ReadTables(DataTable dataTable)
        {
            List<Room> rooms = new List<Room>();

            foreach (DataRow dr in dataTable.Rows)
            {
                Room room = new Room()
                {
                    Id = (int)dr["ID"],
                    Number = (int)dr["RoomID"],
                    Building = dr["Building"].ToString(),
                    RoomType = dr["RoomType"].ToString(),
                    Capacity = (int)dr["RoomCapacity"]
                };
                rooms.Add(room);
            }
            return rooms;
        }
    }
}