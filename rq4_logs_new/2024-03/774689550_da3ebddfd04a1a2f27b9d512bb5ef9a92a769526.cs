using DataAccess.DAOs;
using DTO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAccess.CRUD
{
    public class DoctorCrudFactory: CrudFactory
    {
        public DoctorCrudFactory()
        {
            _dao = SqlDao.GetInstace();
        }

        public override void Create(BaseDTO baseDTO)
        {
            var doctor = baseDTO as Doctor;
            
            var sqlOperation = new SqlOperation { ProcedureName = "CRE_DOCTOR_PR" };
            sqlOperation.AddVarcharParam("P_NAME", doctor.Name);
            sqlOperation.AddVarcharParam("P_LAST_NAME", doctor.LastName);
            sqlOperation.AddIntParam("P_PHONE_NUMBER", doctor.PhoneNumber);
            sqlOperation.AddVarcharParam("P_EMAIL", doctor.Email);
            sqlOperation.AddVarcharParam("P_PASSWORD", doctor.Password);
            sqlOperation.AddVarcharParam("P_SEX", doctor.Sex);
            sqlOperation.AddDatetimeParam("P_BIRTHDATE", doctor.BirthDate);
            sqlOperation.AddVarcharParam("P_ROLE", doctor.Role);
            sqlOperation.AddVarcharParam("P_STATUS", doctor.Status);
            sqlOperation.AddVarcharParam("P_ADDRESS", doctor.Address);
            sqlOperation.AddIntParam("P_BRANCH_ID", doctor.BranchID);

            _dao.ExecuteProcedure(sqlOperation);
        }

        public override void Delete(BaseDTO baseDTO)
        {
            var doctor = baseDTO as Doctor;
            if (doctor == null || doctor.Id == 0)
            {
                throw new ArgumentException("Invalid Id.");
            }

            var SqlOperation = new SqlOperation { ProcedureName = "DEL_DOCTOR_PR" };
            SqlOperation.AddIntParam("P_DOCTOR_ID", doctor.Id);
            _dao.ExecuteProcedure(SqlOperation);
        }

        public override void Update(BaseDTO baseDTO)
        {
            var doctor = baseDTO as Doctor;
            if (doctor == null || doctor.Id == 0)
            {
                throw new ArgumentException("Invalid Id.");
            }

            var sqlOperation = new SqlOperation { ProcedureName = "UPD_DOCTOR_PR" };
            sqlOperation.AddIntParam("P_USERID", doctor.Id);
            sqlOperation.AddVarcharParam("P_NAME", doctor.Name);
            sqlOperation.AddVarcharParam("P_LAST_NAME", doctor.LastName);
            sqlOperation.AddIntParam("P_PHONE_NUMBER", doctor.PhoneNumber);
            sqlOperation.AddVarcharParam("P_EMAIL", doctor.Email);
            sqlOperation.AddVarcharParam("P_PASSWORD", doctor.Password);
            sqlOperation.AddVarcharParam("P_SEX", doctor.Sex);
            sqlOperation.AddDatetimeParam("P_BIRTHDATE", doctor.BirthDate);
            sqlOperation.AddVarcharParam("P_ROLE", doctor.Role);
            sqlOperation.AddVarcharParam("P_STATUS", doctor.Status);
            sqlOperation.AddVarcharParam("P_AdDRESS", doctor.Address);

            _dao.ExecuteProcedure(sqlOperation);
        }


        public override T Retrieve<T>()
        {
            throw new NotImplementedException();
        }

        public override T RetrieveById<T>(int Id)
        {
            var sqlOperation = new SqlOperation() { ProcedureName = "RET_DOCTOR_BY_ID" };
            sqlOperation.AddIntParam("P_USER_ID", Id);
            var lstResult = _dao.ExecuteQueryProcedure(sqlOperation);

            if (lstResult.Count > 0)
            {
                var row = lstResult[0]; 
                var userFound = BuildDoctor(row);
                return (T)Convert.ChangeType(userFound, typeof(T));
            }
            return default(T); 
        }


        public override List<T> RetrieveAll<T>()
        {

            var doctorList = new List<T>();
            var sqlOperation = new SqlOperation() { ProcedureName = "RET_DOCTOR_ALL" };
            var lstResults = _dao.ExecuteQueryProcedure(sqlOperation);

            if (lstResults.Count > 0)
            {
                foreach (var row in lstResults)
                {
                    var doctor = BuildDoctor(row);
                    doctorList.Add((T)Convert.ChangeType(doctor, typeof(T)));
                }
            }
            return doctorList;
        }


        private Doctor BuildDoctor(Dictionary<string, object> row)
        {
            var doctorToReturn = new Doctor()
            {
                Id = (int)row["DOCTOR_ID"],
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
            return doctorToReturn;
        }
    }
}