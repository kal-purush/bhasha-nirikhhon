using Medical_record.Abstractions;
using Medical_record.Data.Models;
using Medical_record.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace Medical_record.Data.MsSqlData
{
    class SqlServerProcedures : IProcedureDataContext
    {
        private readonly ConnectionService _conService;

        public SqlServerProcedures(ConnectionService connectionService)
        {
            _conService = connectionService;
        }

        public async Task<Result<string>> AddProcedureAsync(Procedure procedure)
        {
            if (procedure == null)
            {
                return new Result<string>("Пустое значение параметра");
            }

            var nameProc = @"[dbo].[spProcedures_Add]";
            object res = null;
            try
            {
                using (var con = _conService.GetConnection())
                using (var cmd = new SqlCommand(nameProc, con))
                {
                    var paramName = new SqlParameter();
                    paramName.ParameterName = "@name";
                    paramName.SqlDbType = SqlDbType.NVarChar;
                    paramName.Value = procedure.Name.Trim();

                    var paramDesc = new SqlParameter();
                    paramDesc.ParameterName = "@desc";
                    paramDesc.SqlDbType = SqlDbType.NVarChar;
                    paramDesc.Value = procedure.Description.Trim();

                    cmd.Parameters.Add(paramName);
                    cmd.Parameters.Add(paramDesc);
                    cmd.CommandType = CommandType.StoredProcedure;

                    con.Open();
                    res = await cmd.ExecuteScalarAsync();
                }
            }
            catch (Exception ex)
            {
                return new Result<string>(ex.Message);
            }

            return new Result<string>($"Успешно сохранена новая процедура {res}.", string.Empty);
        }

        public async Task<Result<List<Procedure>>> GetProceduresAsync()
        {
            var procedures = new List<Procedure>();
            var nameProc = @"[dbo].[spProcedures_GetAll]";
            try
            {
                using (var con = _conService.GetConnection())
                using (var cmd = new SqlCommand(nameProc, con))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    con.Open();
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        if (reader.HasRows)
                        {
                            while (await reader.ReadAsync())
                            {
                                var d = new Procedure
                                {
                                    Id = reader.GetInt32(0),
                                    Name = reader.GetString(1),
                                    Description = reader.GetString(2),
                                };
                                procedures.Add(d);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return new Result<List<Procedure>>(ex.Message);
            }

            return new Result<List<Procedure>>(procedures);
        }

        public async Task<Result<List<Procedure>>> GetProceduresLikeAsync(string value)
        {
            if (String.IsNullOrWhiteSpace(value))
            {
                return await GetProceduresAsync();
            }

            var procedures = new List<Procedure>();
            var nameProc = @"[dbo].[spProcedures_GetLike]";
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
                    con.Open();
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        if (reader.HasRows)
                        {
                            while (await reader.ReadAsync())
                            {
                                var d = new Procedure
                                {
                                    Id = reader.GetInt32(0),
                                    Name = reader.GetString(1),
                                    Description = reader.GetString(2),
                                };
                                procedures.Add(d);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return new Result<List<Procedure>>(ex.Message);
            }

            return new Result<List<Procedure>>(procedures);
        }

        public async Task<Result<List<Procedure>>> GetProceduresOrderByAsync(string key)
        {
            if (String.IsNullOrWhiteSpace(key))
            {
                return await GetProceduresAsync();
            }

            var procedures = new List<Procedure>();
            var nameProc = @"[dbo].[spProcedures_GetAllWithOrder]";
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
                    con.Open();
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        if (reader.HasRows)
                        {
                            while (await reader.ReadAsync())
                            {
                                var d = new Procedure
                                {
                                    Id = reader.GetInt32(0),
                                    Name = reader.GetString(1),
                                    Description = reader.GetString(2),
                                };
                                procedures.Add(d);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return new Result<List<Procedure>>(ex.Message);
            }

            return new Result<List<Procedure>>(procedures);
        }

        public async Task<Result<string>> RemoveProcedureAsync(int id)
        {
            if (id == 0)
            {
                return new Result<string>("Невозможно удалить процедуру с Id == 0");
            }

            var nameProc = @"[dbo].[spProcedures_Remove]";
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

                    con.Open();
                    await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                return new Result<string>(ex.Message);
            }

            return new Result<string>($"Успешно удалена процедура.", string.Empty);
        }

        public async Task<Result<string>> UpdateProcedureAsync(Procedure procedure)
        {
            if (procedure == null)
            {
                return new Result<string>("Пустое значение параметра");
            }

            if (procedure.Id == 0)
            {
                return new Result<string>("Невозможно обновить диагноз с Id == 0");
            }

            var nameProc = @"[dbo].[spProcedures_Update]";
            try
            {
                using (var con = _conService.GetConnection())
                using (var cmd = new SqlCommand(nameProc, con))
                {
                    var paramId = new SqlParameter();
                    paramId.ParameterName = "@id";
                    paramId.SqlDbType = SqlDbType.Int;
                    paramId.Value = procedure.Id;

                    var paramName = new SqlParameter();
                    paramName.ParameterName = "@name";
                    paramName.SqlDbType = SqlDbType.NVarChar;
                    paramName.Value = procedure.Name.Trim();

                    var paramDesc = new SqlParameter();
                    paramDesc.ParameterName = "@desc";
                    paramDesc.SqlDbType = SqlDbType.NVarChar;
                    paramDesc.Value = procedure.Description.Trim();

                    cmd.Parameters.Add(paramId);
                    cmd.Parameters.Add(paramName);
                    cmd.Parameters.Add(paramDesc);
                    cmd.CommandType = CommandType.StoredProcedure;

                    con.Open();
                    await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                return new Result<string>(ex.Message);
            }

            return new Result<string>($"Успешно обновлена процедура.", string.Empty);
        }
    }
}