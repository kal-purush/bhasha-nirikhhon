using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Mysqlx.Crud;
using PamerYukLibrary.Database;
using PamerYukLibrary.Entity;

namespace PamerYukLibrary.DAO
{
    public class DAO_GroupChat
    {
        public static List<GroupChat> Select_Group_Chat(string groupName)
        {
            string perintah = "SELECT * FROM chatgroup c inner join groups g  ON c.groupid = g.id WHERE g.nama = '" + groupName + "' and tipePesan != 'Catatan' order by id asc;";
            MySqlDataReader dr = KoneksiDatabase.DatabaseQueryCommand(perintah);
            List<GroupChat> listGroupChat = new List<GroupChat>();
            GroupChat groupChat;
            while (dr.Read())
            {
                //Chat
                int id = int.Parse(dr.GetValue(0).ToString());
                DateTime tglTerkirim = DateTime.Parse(dr.GetValue(1).ToString());
                string pesan = dr.GetValue(2).ToString();
                string tipePesan = dr.GetValue(3).ToString();
                string pengirim = dr.GetValue(4).ToString();
                int group_id_fk = int.Parse(dr.GetValue(5).ToString());

                //Group
                int group_id = int.Parse(dr.GetValue(6).ToString());
                string nama = dr.GetValue(7).ToString();
                string fotoProfil = dr.GetValue(8).ToString();
                DateTime tglDibuat = DateTime.Parse(dr.GetValue(9).ToString());
                string deskripsi = dr.GetValue(10).ToString();

                //Create Group
                Group newGroup = new Group(group_id, nama, fotoProfil, tglDibuat, deskripsi);

                //Create Group Chat
                groupChat = new GroupChat(id, pesan, pengirim, newGroup, tglTerkirim, tipePesan);
                listGroupChat.Add(groupChat);
            }
            return listGroupChat;
        }


        public static List<GroupChat> Select_Catatan_Group(string groupName)
        {
            string perintah = "SELECT * FROM chatgroup c inner join groups g  ON c.groupid = g.id WHERE g.nama = '" + groupName + "' and tipePesan = 'Catatan' order by id asc;";
            MySqlDataReader dr = KoneksiDatabase.DatabaseQueryCommand(perintah);
            List<GroupChat> listGroupChat = new List<GroupChat>();
            GroupChat groupChat;
            while (dr.Read())
            {
                //Chat
                int id = int.Parse(dr.GetValue(0).ToString());
                DateTime tglTerkirim = DateTime.Parse(dr.GetValue(1).ToString());
                string pesan = dr.GetValue(2).ToString();
                string tipePesan = dr.GetValue(3).ToString();
                string pengirim = dr.GetValue(4).ToString();
                int group_id_fk = int.Parse(dr.GetValue(5).ToString());

                //Group
                int group_id = int.Parse(dr.GetValue(6).ToString());
                string nama = dr.GetValue(7).ToString();
                string fotoProfil = dr.GetValue(8).ToString();
                DateTime tglDibuat = DateTime.Parse(dr.GetValue(9).ToString());
                string deskripsi = dr.GetValue(10).ToString();

                //Create Group
                Group newGroup = new Group(group_id, nama, fotoProfil, tglDibuat, deskripsi);

                //Create Group Chat
                groupChat = new GroupChat(id, pesan, pengirim, newGroup, tglTerkirim, tipePesan);
                listGroupChat.Add(groupChat);
            }
            return listGroupChat;
        }

        //Belum kubikin dulu karena takutnya nanti gak bisa ditampilkan di video, berhubung durasu video cuma 5 menit :(
        /*
        public static List<int> Select_GroupChat_ByPesan(string groupName, string pesan)
        {
            string perintah = "SELECT id FROM chatgroup  WHERE pengirim = '" + friend + "' and penerima ='" + user + "' and pesan like '%" + pesan + "%' UNION SELECT id FROM chat  WHERE pengirim = '" + user + "' and penerima ='" + friend + "' and pesan like '%" + pesan + "%' order by id asc;";
            MySqlDataReader dr = KoneksiDatabase.DatabaseQueryCommand(perintah);
            List<int> listChatId = new List<int>();
            while (dr.Read())
            {
                int id = int.Parse(dr.GetValue(0).ToString());
                listChatId.Add(id);
            }
            return listChatId;
        }
        */

        public static void Insert_Group_Chat(GroupChat groupChat)
        {
            string command = "INSERT INTO `pameryuk`.`chatgroup` (`id`, `tglKirim`,`pesan`, `tipePesan`,`pengirim`,`groupId`) VALUES ('" + Get_NewGroupChat_Id() + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','" + groupChat.Pesan + "','" + groupChat.TipePesan + "','" + groupChat.Pengirim + "','" + groupChat.Grup.Id + "');";
            KoneksiDatabase.DatabaseDMLCommand(command);
        }

        private static int Get_NewGroupChat_Id()
        {
            int result = 0;//for sementara
            string perintah = "SELECT id FROM chatgroup ORDER BY id DESC LIMIT 1;";
            MySqlDataReader dr = KoneksiDatabase.DatabaseQueryCommand(perintah);
            if (dr.Read())
            {
                result = int.Parse(dr.GetValue(0).ToString());
            }
            return (result + 1);
        }
    }
}