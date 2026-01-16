using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Dapper;
using SFA.DAS.RoATPService.Application.Interfaces;
using SFA.DAS.RoATPService.Settings;

namespace SFA.DAS.RoATPService.Data
{
    public class EventsRepository : IEventsRepository
    {
        private readonly IWebConfiguration _configuration;

        public EventsRepository(IWebConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task<bool> AddOrganisationStatusEvents(long ukprn, int organisationStatusId, DateTime createdOn)
        {
            using (var connection = new SqlConnection(_configuration.SqlConnectionString))
            {
                if (connection.State != ConnectionState.Open)
                    await connection.OpenAsync();


                var sql = $@"INSERT INTO [dbo].[OrganisationStatusEvent]
                                    ([OrganisationStatusId]
                                    ,[CreatedOn]
                                    ,[ProviderId]) " +
                          "VALUES " +
                          "(@organisationStatusId, @createdOn, @ukprn)";


                var eventsCreated = await connection.ExecuteAsync(sql,
                    new
                    {
                        organisationStatusId,
                        createdOn,
                        ukprn
                    });
                var success = await Task.FromResult(eventsCreated > 0);
                return success;
            }
        }

        public async Task<bool> AddOrganisationStatusEventsFromOrganisationId(Guid organisationId, int organisationStatusId, DateTime createdOn)
        {
            using (var connection = new SqlConnection(_configuration.SqlConnectionString))
            {
                if (connection.State != ConnectionState.Open)
                    await connection.OpenAsync();


                var sql = $@"INSERT INTO [dbo].[OrganisationStatusEvent]
                                    ([OrganisationStatusId]
                                    ,[CreatedOn]
                                    ,[ProviderId]) " +
                          "VALUES " +
                          "(@organisationStatusId, @createdOn, (select top 1 ukprn from organisations where  id=@organisationId))";


                var eventsCreated = await connection.ExecuteAsync(sql,
                    new
                    {
                        organisationStatusId,
                        createdOn,
                        organisationId
                    });
                var success = await Task.FromResult(eventsCreated > 0);
                return success;
            }
        }
    }
}