using DataAccess.DAOs;
using DTO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAccess.CRUD
{
    public class ServiceCrudFactory: CrudFactory
    {
        public ServiceCrudFactory()
        {
            _dao = SqlDao.GetInstace();
        }

        public override void Create(BaseDTO baseDTO)
        {
            var service = baseDTO as Service;

            var sqlOperation = new SqlOperation { ProcedureName = "CRE_SERVICE_PR" };
            sqlOperation.AddVarcharParam("P_NAME", service.Name);
            sqlOperation.AddVarcharParam("P_DESCRIPTION", service.Description);
            sqlOperation.AddIntParam("P_PRICE", service.Price);
            sqlOperation.AddIntParam("P_BRANCH_ID", service.BranchID);

            _dao.ExecuteProcedure(sqlOperation);
        }

        public override void Delete(BaseDTO baseDTO)
        {
            var service = baseDTO as Service;
            if (service == null || service.Id == 0)
            {
                throw new ArgumentException("Invalid Id.");
            }

            var SqlOperation = new SqlOperation { ProcedureName = "DEL_SERVICE_PR" };
            SqlOperation.AddIntParam("P_SERVICE_ID", service.Id);
            _dao.ExecuteProcedure(SqlOperation);
        }

        public override void Update(BaseDTO baseDTO)
        {
            var service = baseDTO as Service;
            if (service == null || service.Id == 0)
            {
                throw new ArgumentException("Invalid Id.");
            }

            var sqlOperation = new SqlOperation { ProcedureName = "UPD_SERVICE_PR" };
            sqlOperation.AddIntParam("P_SERVICE_ID", service.Id);
            sqlOperation.AddVarcharParam("P_NAME", service.Name);
            sqlOperation.AddVarcharParam("P_DESCRIPTION", service.Description);
            sqlOperation.AddIntParam("P_PRICE", service.Price);
            sqlOperation.AddIntParam("P_BRANCH_ID", service.BranchID);

            _dao.ExecuteProcedure(sqlOperation);
        }


        public override T Retrieve<T>()
        {
            throw new NotImplementedException();
        }

        public override T RetrieveById<T>(int Id)
        {
            var sqlOperation = new SqlOperation() { ProcedureName = "RET_SERVICE_BY_ID" };
            sqlOperation.AddIntParam("P_SERVICE_ID", Id);
            var lstResult = _dao.ExecuteQueryProcedure(sqlOperation);

            if (lstResult.Count > 0)
            {
                var row = lstResult[0];
                var srvFound = BuildService(row);
                return (T)Convert.ChangeType(srvFound, typeof(T));
            }
            return default(T);
        }


        public override List<T> RetrieveAll<T>()
        {

            var srvList = new List<T>();
            var sqlOperation = new SqlOperation() { ProcedureName = "RET_SERVICE_ALL" };
            var lstResults = _dao.ExecuteQueryProcedure(sqlOperation);

            if (lstResults.Count > 0)
            {
                foreach (var row in lstResults)
                {
                    var doctor = BuildService(row);
                    srvList.Add((T)Convert.ChangeType(doctor, typeof(T)));
                }
            }
            return srvList;
        }


        private Service BuildService(Dictionary<string, object> row)
        {
            var srvToReturn = new Service()
            {
                Id = (int)row["SERVICE_ID"],
                Name = (string)row["NAME"],
                Description = (string)row["DESCRIPTION"],
                Price = (int)row["PRICE"],
                BranchID = (int)row["BRANCH_ID"],
                /*BranchID = row["BRANCH_ID"] != DBNull.Value ? (int)row["BRANCH_ID"] : 0, // Check for DBNull for integer fields*/
            };
            return srvToReturn;
        }
    }
}