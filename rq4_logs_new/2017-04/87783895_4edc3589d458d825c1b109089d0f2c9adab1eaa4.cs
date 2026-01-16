using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BHXH.HR.Business
{
    public class HrNhanVien
    {
        public static BHXH.Data.HrNhanVien  GetNhanVien(string MaNhanVien)
        {
            BHXH.Data.BHXHEntities ctx = new Data.BHXHEntities();
            var query = from c in ctx.HrNhanVien
                        where c.MaNhanVien.ToString ()  == MaNhanVien.ToString()
                        select c;

            return query.ToList().First();

        }
        
        
        
       
       
        public static BHXH.Data.HrNhanVien  GetAnhNV(string MaNhanVien)
        {
            BHXH.Data.BHXHEntities ctx = new Data.BHXHEntities();
            var query = from c in ctx.HrNhanVien 
                        where c.MaNhanVien.ToString() == MaNhanVien.ToString()
                        select c;

            return query.ToList().First();

        }
       
    }
}