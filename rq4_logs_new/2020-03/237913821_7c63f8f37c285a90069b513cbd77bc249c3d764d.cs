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
    class SqlServerHealthGroups : IHealthGroupDataContext
    {
        private readonly ConnectionService _conService;

        public SqlServerHealthGroups(ConnectionService connectionService)
        {
            _conService = connectionService ??
                throw new ArgumentNullException(nameof(connectionService));
        }

        public async Task<Result<HealthGroup>> GetHealthGroupByIdAsync(int healthGroupId)
        {
            if (healthGroupId == 0)
            {
                return new Result<HealthGroup>("Неверный Id группы здаровья.");
            }

            var healthGroup = new HealthGroup();
            var nameProc = @"[dbo].[spDiagnoses_GetById]";
            try
            {
                using (var con = _conService.GetConnection())
                using (var cmd = new SqlCommand(nameProc, con))
                {
                    var param = new SqlParameter();
                    param.ParameterName = "@id";
                    param.SqlDbType = SqlDbType.Int;
                    param.Value = healthGroupId;

                    cmd.Parameters.Add(param);
                    cmd.CommandType = CommandType.StoredProcedure;
                    await con.OpenAsync();
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            healthGroup.Id = reader.GetInt32(0);
                            healthGroup.Title = reader.GetString(1);
                            healthGroup.Description = reader.GetString(2);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return new Result<HealthGroup>(ex.Message);
            }

            if (healthGroup.Id == 0)
            {
                return new Result<HealthGroup>("Не найден с таким Id");
            }
            else
            {
                return new Result<HealthGroup>(healthGroup);
            }
        }

        public async Task<Result<List<HealthGroup>>> GetHealthGroupsAsync()
        {
            var healthGroups = new List<HealthGroup>();
            var nameProc = @"[dbo].[spHealthGroups_GetAll]";
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
                                var d = new HealthGroup
                                {
                                    Id = reader.GetInt32(0),
                                    Title = reader.GetString(1),
                                    Description = reader.GetString(2),
                                };
                                healthGroups.Add(d);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return new Result<List<HealthGroup>>(ex.Message);
            }

            return new Result<List<HealthGroup>>(healthGroups);
        }
    }
}