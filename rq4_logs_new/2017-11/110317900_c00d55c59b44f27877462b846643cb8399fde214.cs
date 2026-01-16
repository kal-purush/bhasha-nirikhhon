using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using T02_Source_Code.Model;

namespace T02_Source_Code.Bo
{
    class NguoiDungBO
    {
        private bool AddUser(string id, string idTinh, string idHuyen, string idXa, string idChucVu, string name, string pass)
        {
            if (CheckID(id)) return false;
            NguoiDung user = new NguoiDung();
            user.MaNguoiDung = id;
            user.MaTinhThanh = idTinh;
            user.MaQuanHuyen = idHuyen;
            user.MaPhuongXa = idXa;
            user.MaChucVu = idChucVu;
            user.TenNguoiDung = name;
            user.MatKhau = pass;
            DungChung.Db.NguoiDungs.InsertOnSubmit(user);
            DungChung.Db.SubmitChanges();
            return true;
        }
        private void EditUser(string id, string idTinh, string idHuyen, string idXa, string idChucVu, string name, string pass)
        {
            var q = from s in DungChung.Db.NguoiDungs
                    where s.MaNguoiDung.Equals(id)
                    select s;
            if (q.Count() != 0)
            {
                q.First().MaNguoiDung = id;
                q.First().MaTinhThanh = idTinh;
                q.First().MaQuanHuyen = idHuyen;
                q.First().MaPhuongXa = idXa;
                q.First().MaChucVu = idChucVu;
                q.First().TenNguoiDung = name;
                q.First().MatKhau = pass;
                DungChung.Db.SubmitChanges();
            }
        }
        private void DeleteUser(string id)
        {
            var q = from s in DungChung.Db.NguoiDungs
                    where s.MaNguoiDung.Equals(id)
                    select s;
            if (q.Count() != 0)
            {
                DungChung.Db.NguoiDungs.DeleteOnSubmit(q.First());
                DungChung.Db.SubmitChanges();
            }
        }
        /// <summary>
        /// Kiểm tra có trùng id người dùng hay k. Nếu trùng -> true
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool CheckID(string id)
        {
            var q = from s in DungChung.Db.NguoiDungs
                    where s.MaNguoiDung.Equals(id)
                    select s;
            return q.Count() == 0 ? false : true;
        }
        /// <summary>
        /// Lấy danh sách user của những user ở những tầng thấp hơn
        /// </summary>
        /// <param name="id">id người dùng hiện tại</param>
        /// <returns></returns>
        public List<NguoiDung> getListUser(string id )
        {
            NguoiDung user = getNguoiDungByID(id);
            if (user.MaTinhThanh == null)           
                return (from s in DungChung.Db.NguoiDungs select s).ToList();       
            else if (user.MaQuanHuyen == null)
            {
                var q= from 
            }

            var q = from s in DungChung.Db.NguoiDungs
                    where s.MaTinhThanh == null
                    select s;
                    
            return DungChung.Db.NguoiDungs.ToList();
        }
        /// <summary>
        /// Lấy ra 1 người dùng theo mã người dùng
        /// </summary>
        /// <param name="id">mã người dùng</param>
        /// <returns></returns>
        public NguoiDung getNguoiDungByID(string id)
        {
            var q = from s in DungChung.Db.NguoiDungs
                    where s.MaNguoiDung.Equals(id)
                    select s;
            if (q.Count() != 0)
                return q.First();
            return null;
        }
    }
}