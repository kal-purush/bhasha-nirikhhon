using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using ESFA.DC.CrossLoad;
using ESFA.DC.CrossLoad.Dto;
using ESFA.DC.CrossLoad.Message;
using ESFA.DC.JobQueueManager.Interfaces;
using ESFA.DC.Jobs.Model;
using ESFA.DC.Jobs.Model.Enums;
using ESFA.DC.JobScheduler.Settings;
using ESFA.DC.JobStatus.Interface;
using ESFA.DC.Queueing.Interface;
using Microsoft.EntityFrameworkCore;

namespace ESFA.DC.JobSchduler.CrossLoading
{
    public class CrossLoadingService : ICrossLoadingService
    {
        private readonly IQueuePublishService<MessageCrossLoadDctToDcftDto> _queuePublishService;
        private readonly IFileUploadJobManager _jobQueueManager;
        private readonly CrossLoadMessageMapper _crossLoadMessageMapper;
        private readonly string _connectionString;

        public CrossLoadingService(
            CrossLoadMessageMapper crossLoadMessageMapper,
            IQueuePublishService<MessageCrossLoadDctToDcftDto> queuePublishService,
            IFileUploadJobManager jobQueueManager,
            ConnectionStrings connectionStrings)
        {
            _queuePublishService = queuePublishService;
            _jobQueueManager = jobQueueManager;
            _crossLoadMessageMapper = crossLoadMessageMapper;
            _connectionString = connectionStrings.Organisation;
        }

        public async Task SendMessageForCrossLoading(long jobId)
        {
            var job = _jobQueueManager.GetJobById(jobId);

            if (job.JobType == JobType.IlrSubmission && job.IsFirstStage)
            {
                return;
            }

            var upin = await GetUpin(job.Ukprn);
            var reportsFileName = $"{job.Ukprn}/{job.JobId}/ReportsDC";
            var message = new MessageCrossLoadDctToDcft(
                job.JobId,
                job.Ukprn,
                upin,
                job.StorageReference,
                job.FileName,
                MapJobType(job.JobType),
                job.SubmittedBy,
                $"{reportsFileName}1.zip",
                $"{reportsFileName}2.zip");

            await _queuePublishService.PublishAsync(_crossLoadMessageMapper.FromMessage(message));
        }

        public async Task<long> GetUpin(long ukprn)
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                var parameters = new DynamicParameters();
                parameters.Add("@ukprn", ukprn, DbType.Int32, ParameterDirection.Input);

                await sqlConnection.OpenAsync();
                var upin = await sqlConnection.ExecuteScalarAsync<long>("SELECT top 1 [Upin] FROM [Org_UKPRN_UPIN] WHERE [Ukprn] = @ukprn And Status='Active'", parameters);
                return upin;
            }
        }

        public CrossLoadJobType MapJobType(JobType jobType)
        {
            switch (jobType)
            {
                case JobType.IlrSubmission:
                    return CrossLoadJobType.ILR;
                case JobType.EsfSubmission:
                    return CrossLoadJobType.ESF;
                case JobType.EasSubmission:
                    return CrossLoadJobType.EAS;
                default:
                    throw new Exception("unknown job type");
            }
        }
    }
}