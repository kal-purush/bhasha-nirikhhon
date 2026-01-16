using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuanLyThuVien
{
    class classXoa
    {
        public static bool XoaSach(string ID)
        {
            bool res = connect.DeleteSach(ID);
            // truy vẫn csdl , sửa sách có id = ID thành new_sach thành công return 1, ko thành công return 0
            return res;
        }
        public static bool XoaDocGia(string ID)
        {
            bool res = connect.DeleteDocgia(ID);
            return res;
        }
    }
}