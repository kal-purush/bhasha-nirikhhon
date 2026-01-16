using Medical_record.Abstractions;
using Medical_record.Data.Models;
using Medical_record.Utils;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Medical_record.Data.MsSqlData
{
    public class SqlServerDoctors : IDoctorDataContext
    {
        private readonly ConnectionService _conService;

        public SqlServerDoctors(ConnectionService connectionService)
        {
            _conService = connectionService
                ?? throw new ArgumentNullException(nameof(connectionService));
        }

        public async Task<Result<List<Doctor>>> GetDoctorsAsync()
        {
            var docs = new List<Doctor>();
            var nameProc = @"[dbo].[spDoctors_GetAll]";
            try
            {
                using (var con = _conService.GetConnection())
                using (var cmd = new SqlCommand(nameProc, con))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    await con.OpenAsync();
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        if (reader.HasRows)
                        {
                            while (await reader.ReadAsync())
                            {
                                Doctor d = GetDoctorFromReader(reader);
                                docs.Add(d);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return new Result<List<Doctor>>(ex.Message);
            }

            return new Result<List<Doctor>>(docs);
        }

        public async Task<Result<Doctor>> GetDoctorByIdAsync(int doctorId)
        {
            if (doctorId == 0)
            {
                return new Result<Doctor>("Неверный Id врача.");
            }

            var doc = new Doctor();
            var nameProc = @"[dbo].[spDoctors_GetById]";
            try
            {
                using (var con = _conService.GetConnection())
                using (var cmd = new SqlCommand(nameProc, con))
                {
                    var param = new SqlParameter();
                    param.ParameterName = "@id";
                    param.SqlDbType = SqlDbType.Int;
                    param.Value = doctorId;

                    cmd.Parameters.Add(param);
                    cmd.CommandType = CommandType.StoredProcedure;
                    await con.OpenAsync();
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            doc = GetDoctorFromReader(reader);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return new Result<Doctor>(ex.Message);
            }

            if (doc.Id == 0)
            {
                return new Result<Doctor>("Не найден с таким Id");
            }
            else
            {
                return new Result<Doctor>(doc);
            }
        }

        public async Task<Result<string>> AddDoctorAsync(Doctor doctor)
        {
            if (doctor == null)
            {
                return new Result<string>("Пустое значение параметра");
            }

            var nameProc = @"[dbo].[spDoctors_Add]";
            object res = null;
            try
            {
                var parameters = GetSqlParametersFromDoctor(doctor);

                using (var con = _conService.GetConnection())
                using (var cmd = new SqlCommand(nameProc, con))
                {
                    cmd.Parameters.AddRange(parameters);
                    cmd.CommandType = CommandType.StoredProcedure;

                    await con.OpenAsync();
                    res = await cmd.ExecuteScalarAsync();
                }
            }
            catch (Exception ex)
            {
                return new Result<string>(ex.Message);
            }

            return new Result<string>($"Успешно сохранен новый диагноз {res}.", string.Empty);
        }

        private Doctor GetDoctorFromReader(SqlDataReader reader)
        {
            var result = new Doctor
            {
                Id = reader.GetInt32(0),
                LastName = reader.GetString(1),
                FirstName = reader.GetString(2),
                MiddleName = reader.GetString(3),
                SpecializationId = reader.GetInt32(4)
            };

            result.Specialization = new Specialization
            {
                Id = reader.GetInt32(4),
                Name = reader.GetString(5)
            };

            return result;
        }

        private SqlParameter[] GetSqlParametersFromDoctor(Doctor doctor)
        {
            var result = new List<SqlParameter>();

            var paraml = new SqlParameter();
            paraml.ParameterName = "@lName";
            paraml.SqlDbType = SqlDbType.NVarChar;
            paraml.Value = doctor.LastName.Trim();

            var paramf = new SqlParameter();
            paramf.ParameterName = "@fName";
            paramf.SqlDbType = SqlDbType.NVarChar;
            paramf.Value = doctor.FirstName.Trim();

            var paramm = new SqlParameter();
            paramm.ParameterName = "@mName";
            paramm.SqlDbType = SqlDbType.NVarChar;
            paramm.Value = doctor.MiddleName.Trim();

            var paramS = new SqlParameter();
            paramS.ParameterName = "@specId";
            paramS.SqlDbType = SqlDbType.Int;
            paramS.Value = doctor.SpecializationId;

            result.Add(paraml);
            result.Add(paramf);
            result.Add(paramm);
            result.Add(paramS);
            return result.ToArray();
        }
    }
}