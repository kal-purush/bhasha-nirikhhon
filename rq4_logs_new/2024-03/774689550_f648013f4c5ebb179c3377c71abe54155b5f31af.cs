using DataAccess.DAOs;
using DTO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAccess.CRUD
{
    public class SecretaryCrudFactory: CrudFactory
    {
        public SecretaryCrudFactory()
        {
            _dao = SqlDao.GetInstace();
        }

        public override void Create(BaseDTO baseDTO)
        {
            var secretary = baseDTO as Secretary;

            var sqlOperation = new SqlOperation { ProcedureName = "CRE_SECRETARY_PR" };
            sqlOperation.AddVarcharParam("P_NAME", secretary.Name);
            sqlOperation.AddVarcharParam("P_LAST_NAME", secretary.LastName);
            sqlOperation.AddIntParam("P_PHONE_NUMBER", secretary.PhoneNumber);
            sqlOperation.AddVarcharParam("P_EMAIL", secretary.Email);
            sqlOperation.AddVarcharParam("P_PASSWORD", secretary.Password);
            sqlOperation.AddVarcharParam("P_SEX", secretary.Sex);
            sqlOperation.AddDatetimeParam("P_BIRTHDATE", secretary.BirthDate);
            sqlOperation.AddVarcharParam("P_ROLE", secretary.Role);
            sqlOperation.AddVarcharParam("P_STATUS", secretary.Status);
            sqlOperation.AddVarcharParam("P_ADDRESS", secretary.Address);
            sqlOperation.AddIntParam("P_BRANCH_ID", secretary.BranchID);

            _dao.ExecuteProcedure(sqlOperation);
        }

        public override void Delete(BaseDTO baseDTO)
        {
            var secretary = baseDTO as Secretary;
            if (secretary == null || secretary.Id == 0)
            {
                throw new ArgumentException("Invalid Id.");
            }

            var SqlOperation = new SqlOperation { ProcedureName = "DEL_SECRETARY_PR" };
            SqlOperation.AddIntParam("P_SECRETARY_ID", secretary.Id);
            _dao.ExecuteProcedure(SqlOperation);
        }

        public override void Update(BaseDTO baseDTO)
        {
            var secretary = baseDTO as Secretary;
            if (secretary == null || secretary.Id == 0)
            {
                throw new ArgumentException("Invalid Id.");
            }

            var sqlOperation = new SqlOperation { ProcedureName = "UPD_SECRETARY_PR" };
            sqlOperation.AddIntParam("P_SECRETARY_ID", secretary.Id);
            sqlOperation.AddIntParam("P_BRANCH_ID", secretary.BranchID);
            sqlOperation.AddVarcharParam("P_NAME", secretary.Name);
            sqlOperation.AddVarcharParam("P_LAST_NAME", secretary.LastName);
            sqlOperation.AddIntParam("P_PHONE_NUMBER", secretary.PhoneNumber);
            sqlOperation.AddVarcharParam("P_EMAIL", secretary.Email);
            sqlOperation.AddVarcharParam("P_PASSWORD", secretary.Password);
            sqlOperation.AddVarcharParam("P_SEX", secretary.Sex);
            sqlOperation.AddDatetimeParam("P_BIRTHDATE", secretary.BirthDate);
            sqlOperation.AddVarcharParam("P_ROLE", secretary.Role);
            sqlOperation.AddVarcharParam("P_STATUS", secretary.Status);
            sqlOperation.AddVarcharParam("P_ADDRESS", secretary.Address);

            _dao.ExecuteProcedure(sqlOperation);
        }


        public override T Retrieve<T>()
        {
            throw new NotImplementedException();
        }

        public override T RetrieveById<T>(int Id)
        {
            var sqlOperation = new SqlOperation() { ProcedureName = "RET_SECRETARY_BY_ID" };
            sqlOperation.AddIntParam("P_SECRETARY_ID", Id);
            var lstResult = _dao.ExecuteQueryProcedure(sqlOperation);

            if (lstResult.Count > 0)
            {
                var row = lstResult[0];
                var userFound = BuildSecretary(row);
                return (T)Convert.ChangeType(userFound, typeof(T));
            }
            return default(T);
        }


        public override List<T> RetrieveAll<T>()
        {

            var secretaryList = new List<T>();
            var sqlOperation = new SqlOperation() { ProcedureName = "RET_SECRETARY_ALL" };
            var lstResults = _dao.ExecuteQueryProcedure(sqlOperation);

            if (lstResults.Count > 0)
            {
                foreach (var row in lstResults)
                {
                    var secretary = BuildSecretary(row);
                    secretaryList.Add((T)Convert.ChangeType(secretary, typeof(T)));
                }
            }
            return secretaryList;
        }


        private Secretary BuildSecretary(Dictionary<string, object> row)
        {
            var secretaryToReturn = new Secretary()
            {
                Id = (int)row["SECRETARY_ID"],
                Name = (string)row["NAME"],
                LastName = (string)row["LAST_NAME"],
                PhoneNumber = (int)row["PHONE_NUMBER"],
                Email = (string)row["EMAIL"],
                Password = (string)row["PASSWORD"],
                Sex = (string)row["SEX"],
                BirthDate = (DateTime)row["BIRTHDATE"],
                Role = (string)row["ROLE"],
                Status = (string)row["STATUS"],
                Address = (string)row["ADDRESS"],
                Created = (DateTime)row["CREATED"],
                BranchID = (int)row["BRANCH_ID"],
                /*BranchID = row["BRANCH_ID"] != DBNull.Value ? (int)row["BRANCH_ID"] : 0, // Check for DBNull for integer fields*/
            };
            return secretaryToReturn;
        }
    }
}