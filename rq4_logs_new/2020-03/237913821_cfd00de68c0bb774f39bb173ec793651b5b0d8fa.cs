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
    public class SqlServerMedications : IMedicationsDataContext
    {
        private readonly ConnectionService _conService;

        public SqlServerMedications(ConnectionService connectionService)
        {
            _conService = connectionService ??
                throw new ArgumentNullException(nameof(connectionService));
        }

        public async Task<Result<List<Medications>>> GetMedicationsAsync()
        {
            var medics = new List<Medications>();
            var nameProc = @"[dbo].[spMedications_GetAll]";
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
                                var m = GetMedicationsFromReader(reader);
                                medics.Add(m);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return new Result<List<Medications>>(ex.Message);
            }

            return new Result<List<Medications>>(medics);
        }

        public async Task<Result<List<Medications>>> GetMedicationsLikeAsync(string value)
        {
            if (String.IsNullOrWhiteSpace(value))
            {
                return await GetMedicationsAsync();
            }

            var medics = new List<Medications>();
            var nameProc = @"[dbo].[spMedications_GetLike]";
            try
            {
                using (var con = _conService.GetConnection())
                using (var cmd = new SqlCommand(nameProc, con))
                {
                    var param = new SqlParameter();
                    param.ParameterName = "@value";
                    param.SqlDbType = SqlDbType.NVarChar;
                    param.Value = value;

                    cmd.Parameters.Add(param);
                    cmd.CommandType = CommandType.StoredProcedure;
                    await con.OpenAsync();
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        if (reader.HasRows)
                        {
                            while (await reader.ReadAsync())
                            {
                                var m = GetMedicationsFromReader(reader);
                                medics.Add(m);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return new Result<List<Medications>>(ex.Message);
            }

            return new Result<List<Medications>>(medics);
        }

        public async Task<Result<List<Medications>>> GetMedicationsOrderByAsync(string key)
        {
            if (String.IsNullOrWhiteSpace(key))
            {
                return await GetMedicationsAsync();
            }

            var medics = new List<Medications>();
            var nameProc = @"[dbo].[spMedications_GetAllWithOrder]";
            try
            {
                using (var con = _conService.GetConnection())
                using (var cmd = new SqlCommand(nameProc, con))
                {
                    var param = new SqlParameter();
                    param.ParameterName = "@key";
                    param.SqlDbType = SqlDbType.VarChar;
                    param.Value = key;

                    cmd.Parameters.Add(param);
                    cmd.CommandType = CommandType.StoredProcedure;
                    await con.OpenAsync();
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        if (reader.HasRows)
                        {
                            while (await reader.ReadAsync())
                            {
                                var m = GetMedicationsFromReader(reader);
                                medics.Add(m);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return new Result<List<Medications>>(ex.Message);
            }

            return new Result<List<Medications>>(medics);
        }

        public async Task<Result<string>> AddMedicationsAsync(Medications medications)
        {
            if (medications == null)
            {
                return new Result<string>("Пустое значение параметра");
            }

            var parameters = GetParametersFromMedications(medications);
            var nameProc = @"[dbo].[spMedications_Add]";
            object res = null;
            try
            {
                using (var con = _conService.GetConnection())
                using (var cmd = new SqlCommand(nameProc, con))
                {
                    parameters.ForEach(p => cmd.Parameters.Add(p));
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

        public async Task<Result<string>> UpdateMedicationsAsync(Medications medications)
        {
            if (medications == null)
            {
                return new Result<string>("Пустое значение параметра");
            }

            if (medications.Id == 0)
            {
                return new Result<string>("Невозможно обновить лекарство с Id == 0");
            }

            var parameters = GetParametersFromMedications(medications);
            var nameProc = @"[dbo].[spMedications_Update]";
            try
            {
                using (var con = _conService.GetConnection())
                using (var cmd = new SqlCommand(nameProc, con))
                {
                    var paramId = new SqlParameter();
                    paramId.ParameterName = "@id";
                    paramId.SqlDbType = SqlDbType.Int;
                    paramId.Value = medications.Id;

                    cmd.Parameters.Add(paramId);
                    parameters.ForEach(p => cmd.Parameters.Add(p));
                    cmd.CommandType = CommandType.StoredProcedure;

                    await con.OpenAsync();
                    await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                return new Result<string>(ex.Message);
            }

            return new Result<string>($"Успешно обновлено лекарство.", string.Empty);
        }

        public async Task<Result<string>> RemoveMedicationsAsync(int id)
        {
            if (id == 0)
            {
                return new Result<string>("Невозможно удалить лекарство с Id == 0");
            }

            var nameProc = @"[dbo].[spMedications_Remove]";
            try
            {
                using (var con = _conService.GetConnection())
                using (var cmd = new SqlCommand(nameProc, con))
                {
                    var paramId = new SqlParameter();
                    paramId.ParameterName = "@id";
                    paramId.SqlDbType = SqlDbType.Int;
                    paramId.Value = id;

                    cmd.Parameters.Add(paramId);
                    cmd.CommandType = CommandType.StoredProcedure;

                    await con.OpenAsync();
                    await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                return new Result<string>(ex.Message);
            }

            return new Result<string>($"Успешно удалено лекарство.", string.Empty);
        }

        private static Medications GetMedicationsFromReader(SqlDataReader reader)
        {
            return new Medications
            {
                Id = reader.GetInt32(0),
                Name = reader.GetString(1),
                Description = reader.GetString(2),
                QuantityPackage = reader.GetInt32(3),
                RestPackages = reader.GetInt32(4),
                ArrivalPackages = reader.GetInt32(5),
                RemainedUnits = reader.GetInt32(6),
                ArrivalDate = reader.GetDateTime(7),
                ShelfLife = reader.GetDateTime(8)
            };
        }

        private List<SqlParameter> GetParametersFromMedications(Medications medications)
        {
            var paramName = new SqlParameter
            {
                ParameterName = "@name",
                SqlDbType = SqlDbType.NVarChar,
                Value = medications.Name.Trim()
            };

            var paramDesc = new SqlParameter
            {
                ParameterName = "@desc",
                SqlDbType = SqlDbType.NVarChar,
                Value = medications.Description.Trim()
            };

            var paramQP = new SqlParameter
            {
                ParameterName = "@qp",
                SqlDbType = SqlDbType.Int,
                Value = medications.QuantityPackage
            };

            var paramRP = new SqlParameter
            {
                ParameterName = "@rp",
                SqlDbType = SqlDbType.Int,
                Value = medications.RestPackages
            };

            var paramAP = new SqlParameter
            {
                ParameterName = "@ap",
                SqlDbType = SqlDbType.Int,
                Value = medications.ArrivalPackages
            };

            var paramRU = new SqlParameter
            {
                ParameterName = "@ru",
                SqlDbType = SqlDbType.Int,
                Value = medications.RemainedUnits
            };

            var paramAD = new SqlParameter
            {
                ParameterName = "@ad",
                SqlDbType = SqlDbType.DateTime,
                Value = medications.ArrivalDate
            };

            var paramSL = new SqlParameter
            {
                ParameterName = "@sl",
                SqlDbType = SqlDbType.DateTime,
                Value = medications.ShelfLife
            };

            var result = new List<SqlParameter>
            { paramName, paramDesc, paramQP, paramRP, paramAP, paramRU, paramAD, paramSL};
            
            return result;
        }
    }
}