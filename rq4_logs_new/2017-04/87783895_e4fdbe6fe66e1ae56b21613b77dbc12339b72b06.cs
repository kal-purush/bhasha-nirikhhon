using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BHXH.List.Business
{
    public class ListNationality
    {
        public static IEnumerable<BHXH.Data.ListNationality> GetAll()
        {
            BHXH.Data.BHXHEntities ctx = new Data.BHXHEntities();
            IEnumerable<BHXH.Data.ListNationality> list;
            var query = from c in ctx.ListNationality select c;
            list = query.ToList();
            return list;
        }

        public static BHXH.Data.ListNationality New(string NationalityCode, string NationalityName)
        {
            try
            {

                BHXH.Data.BHXHEntities ctx = new Data.BHXHEntities();
            BHXH.Data.ListNationality n = new Data.ListNationality();
            n.NationalityCode = NationalityCode;
            n.NationalityName = NationalityName;

            ctx.ListNationality.Add(n);
               ctx.SaveChanges();
                return n;
            }
            //catch (Exception)
            //{ return n; }
            finally
            { }



        }
        public static void Edit( string NationalityCode, string NationalityName)
        {

        }
    }
}