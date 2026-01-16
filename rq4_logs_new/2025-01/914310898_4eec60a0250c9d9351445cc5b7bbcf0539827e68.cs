using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PamerYukLibrary.Entity
{
    public class Group
    {
        private int id;
        private List<User> members = new List<User>();
        private string fotoProfil;
        private DateTime tglDibuat;
        private string deskripsi;

        public Group(int id, List<User> members, string fotoProfil, DateTime tglDibuat, string deskripsi)
        {
            this.Id = id;
            this.Members = members;
            this.FotoProfil = fotoProfil;
            this.TglDibuat = tglDibuat;
            this.Deskripsi = deskripsi;
        }

        public int Id { get => id; set => id = value; }
        public List<User> Members { get => members; set => members = value; }
        public string FotoProfil { get => fotoProfil; set => fotoProfil = value; }
        public DateTime TglDibuat { get => tglDibuat; set => tglDibuat = value; }
        public string Deskripsi { get => deskripsi; set => deskripsi = value; }
    }
}