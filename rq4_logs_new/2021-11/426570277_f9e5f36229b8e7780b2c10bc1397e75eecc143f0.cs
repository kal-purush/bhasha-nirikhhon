using Admin.Application.Contracts;
using Admin.Application.Features.Queries.SearchQuery;
using Admin.Domain.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using SkillTracker.Entities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Admin.Application.Services
{
    public class SearchService : ISearchService
    {
        private readonly IPersonalInfoProvider _personalInfoProvider;
        private readonly ISkillProvider _skillProvider;
        private readonly ICacheProvider _cacheProvider;
        private readonly ICacheRepository _cacheRepo;
        private readonly AutoMapper.IMapper _mapper;
        private readonly ILogger<SearchService> _logger;

        private readonly bool _cacheEnabled;

        public SearchService(
            ICacheProvider cacheProvider,
            ICacheRepository cacheRepo,
            ISkillProvider skillProvider,
            IPersonalInfoProvider personalInfoProvider,
            AutoMapper.IMapper mapper,
            ILogger<SearchService> logger,
            IConfiguration configuration
        )
        {
            _cacheProvider = cacheProvider;
            _cacheRepo = cacheRepo;
            _skillProvider = skillProvider;
            _personalInfoProvider = personalInfoProvider;
            _mapper = mapper;
            _logger = logger;

            bool.TryParse(configuration["CacheEnabled"], out _cacheEnabled);
        }

        public async Task<IEnumerable<Profile>> Search(SearchProfileQuery query)
        {
            if (!string.IsNullOrWhiteSpace(query.EmpId))
            {
                var response = new List<Profile>();
                var profile =  await SearchById(query.EmpId);
                if (profile != null)
                {
                    response.Add(profile);
                }
                return response;
            }
            else if (!string.IsNullOrWhiteSpace(query.Name))
            {
                return await SearchByName(query.Name);
            }
            else if (!string.IsNullOrWhiteSpace(query.Skill))
            {
                return await SearchBySkillName(query.Skill);
            }
            return new List<Profile>();
        }

        private async Task<Profile> SearchById(string empId)
        {
            var cachedData = GetFromCache(empId);
            if (cachedData != null)
            {
                return cachedData;
            }
            var personalInfo = await _personalInfoProvider.SearchByEmpIdAsync(empId);
            if (personalInfo != null)
            {
                var profile = await GetProfile(personalInfo);
                if (profile != null)
                {
                    await SetToCache(empId, profile);
                    return profile;
                }
            }

            return null;
        }
        private async Task<IEnumerable<Profile>> SearchByName(string name)
        {
            ConcurrentBag<Profile> profiles = new ConcurrentBag<Profile>();

            var empIdList = await _personalInfoProvider.GetEmployeeIdsByname(name);

            var tasks = empIdList.Select(async id =>
            {
                var profile = await SearchById(id);
                profiles.Add(profile);
            });
            await Task.WhenAll(tasks);

            return profiles;
        }

        private async Task<IEnumerable<Profile>> SearchBySkillName(string skillName)
        {
            ConcurrentBag<Profile> profiles = new ConcurrentBag<Profile>();

            var empIdList = await _skillProvider.SearchForKeyAsync(skillName);

            var tasks = empIdList.Select(async id =>
            {
                var profile = await SearchById(id);
                profiles.Add(profile);
            });
            await Task.WhenAll(tasks);

            return profiles;
        }


        private async Task<Profile> GetProfile(PersonalInfoEntity personalInfo)
        {
            var skills = await _skillProvider.GetAsync(personalInfo.EmpId);
            var profile = _mapper.Map<Profile>(personalInfo);
            profile.Skills = skills.Select(s => _mapper.Map<Skill>(s)).ToList();
            return profile;
        }

        private Profile GetFromCache(string key)
        {
            return _cacheEnabled ? _cacheProvider.GetCache<Profile>(key) : null;
        }

        private async Task SetToCache(string key, Profile data)
        {
            if (_cacheEnabled)
            {
                await _cacheRepo.SetAsync<Profile>(key, data);
            }
        }
    }
}