using ClinicManager.DataAccess;
using ClinicManager.DataModel;
using System.Data;
using System.Linq;

namespace ClinicManager.DataAccess
{
    public class BillAccess : BaseDataAccess
    {
        public override int Insert(object obj)
        {
            int result = base.Insert(obj);
            if (result > 0)
            {
                using (var db = new ClinicDB())
                {
                    var query = from d in db.Bill orderby d.BillID descending select d;
                    var last = query.First();
                    result = last.BillID;
                }
            }

            return result;
        }


        public bool Delete(int id)
        {
            Bill model;
            using (var db = new ClinicDB())
            {
                model = db.Bill.Find(id);

                if (model == null)
                {
                    return false;
                }
            }

            return base.Delete(model);
        }

        public DataTable GetAll()
        {
            using (var db = new ClinicDB())
            {
                return db.Bill.ToDataTable();
            }
        }
    }
}