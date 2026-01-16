using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TTN_QL_HSGV.DAL;
using TTN_QL_HSGV.DTO;

namespace TTN_QL_HSGV.BUS
{
    class DanhSachLopBUS : IConverter<Lop>
    {
        public DataTable GetLopData()
        {
            return DataProvider.Instance.ExecuteQuery("Gọi hàm trả về lớp ở đây này");
        }
        
        public List<Lop> Convert(DataTable datatable)
        {
            
            return new List<Lop>();
        }
    }
}