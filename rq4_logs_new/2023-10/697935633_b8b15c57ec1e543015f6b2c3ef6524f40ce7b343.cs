using vabalas_api.Controllers.Statistics;
using vabalas_api.Enums;
using vabalas_api.Repositories;
using vabalas_api.Repositories.Impl;
using vabalas_api.Utils;

namespace vabalas_api.Service.Impl;

public class StatisticsService : IStatisticsService
{
    private readonly IJobService _jobService;

    private readonly IJobRepository _jobRepository;

    public StatisticsService(IJobService jobService, IJobRepository jobRepository)
    {
        _jobService = jobService;
        _jobRepository = jobRepository;
    }

    public async Task<int> GetTotalAmountOfJobs()
    {
        var jobs = await _jobService.FindAll();
        return jobs.Count();
    }

    public async Task<int> GetTotalAmountOfJobsByCategory(string category)
    {
        JobCategory jobCategory = JobCategoryHelper.ParseToEnum(category);
        var jobs = await _jobRepository.GetAllByCategory(jobCategory);
        return jobs.Count;
    }

    public async Task<List<CategoryDistributionDto>> GetCategoryDistribution()
    {
        var categoryDistributionList = new List<CategoryDistributionDto>();
        var totalJobs = await GetTotalAmountOfJobs();
        foreach (JobCategory category in Enum.GetValues(typeof(JobCategory)))
        {
            var categoryDistributionDto = new CategoryDistributionDto();
            var currentCategoryJobs = (await _jobRepository.GetAllByCategory(category)).Count;
            categoryDistributionDto.JobCategory = category;
            categoryDistributionDto.Percentage = PercentageHelper.GetPercentage(totalJobs, currentCategoryJobs);
            
            categoryDistributionList.Add(categoryDistributionDto);
        }

        return categoryDistributionList;
    }
}