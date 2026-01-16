using Co.Id.Moonlay.Simple.Auth.Service.Lib.BusinessLogic.Interfaces;
using Co.Id.Moonlay.Simple.Auth.Service.Lib.Models;
using Co.Id.Moonlay.Simple.Auth.Service.Lib.Services.IdentityService;
using Co.Id.Moonlay.Simple.Auth.Service.Lib.Utilities;
using Com.Moonlay.Models;
using Com.Moonlay.NetCore.Lib;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Co.Id.Moonlay.Simple.Auth.Service.Lib.BusinessLogic.Services
{
    public class EducationInfoService : IEducationInfoService
    {
        private const string UserAgent = "auth-service";
        protected DbSet<EducationInfo> DbSet;
        protected IIdentityService IdentityService;
        public AuthDbContext DbContext;

        public EducationInfoService(IServiceProvider serviceProvider, AuthDbContext dbContext)
        {
            DbContext = dbContext;
            this.DbSet = dbContext.Set<EducationInfo>();
        }
        public bool CheckDuplicate(int id, string educationinfoid)
        {
            return DbSet.Any(r => r.IsDeleted.Equals(false) && r.Id != id && r.EducationInfoId.Equals(educationinfoid));
        }

        public async Task<int> CreateAsync(EducationInfo model)
        {
            EntityExtension.FlagForCreate(model, IdentityService.Username, UserAgent);
            DbSet.Add(model);
            return await DbContext.SaveChangesAsync();
        }

        public async Task<int> DeleteAsync(int id)
        {
            EducationInfo model = await ReadByIdAsync(id);
            EntityExtension.FlagForDelete(model, IdentityService.Username, UserAgent, true);
            DbSet.Update(model);
            return await DbContext.SaveChangesAsync();
        }

        public ReadResponse<EducationInfo> Read(int page, int size, string order, List<string> select, string keyword, string filter)
        {
            IQueryable<EducationInfo> query = DbSet.Where(x => !x.IsDeleted);
            List<string> searchAttributes = new List<string>()
            {
                "EducationInfoId"
            };
            query = QueryHelper<EducationInfo>.Search(query, searchAttributes, keyword);

            Dictionary<string, object> filterDictionary = JsonConvert.DeserializeObject<Dictionary<string, object>>(filter);
            query = QueryHelper<EducationInfo>.Filter((IQueryable<EducationInfo>)query, filterDictionary);

            List<string> selectedFields = new List<string>()
                {
                    "EducationInfoId", "Grade","Institution",
                };
            Dictionary<string, string> orderDictionary = JsonConvert.DeserializeObject<Dictionary<string, string>>(order);
            query = QueryHelper<EducationInfo>.Order((IQueryable<EducationInfo>)query, orderDictionary);

            query = query.Select(x => new EducationInfo()
            {
                Active = x.Active,
                CreatedAgent = x.CreatedAgent,
                CreatedBy = x.CreatedBy,
                CreatedUtc = x.CreatedUtc,
                DeletedAgent = x.DeletedAgent,
                DeletedBy = x.DeletedBy,
                DeletedUtc = x.DeletedUtc,
                Id = x.Id,
                IsDeleted = x.IsDeleted,
                LastModifiedAgent = x.LastModifiedAgent,
                LastModifiedBy = x.LastModifiedBy,
                LastModifiedUtc = x.LastModifiedUtc,
            });

            Pageable<EducationInfo> pageable = new Pageable<EducationInfo>(query, page - 1, size);
            List<EducationInfo> data = pageable.Data.ToList();
            int totalData = pageable.TotalCount;

            return new ReadResponse<EducationInfo>(data, totalData, orderDictionary, selectedFields);
        }

        public async Task<EducationInfo> ReadByIdAsync(int id)
        {
            var result = await DbSet.FirstOrDefaultAsync(d => d.Id.Equals(id) && !d.IsDeleted);
            return result;
        }

        public async Task<int> UpdateAsync(int id, EducationInfo model)
        {
            var data = await ReadByIdAsync(id);

            //Formal Education Experience
            data.Grade = model.Grade;
            data.Institution = model.Institution;
            data.Majors = model.Majors;
            data.YearStart = model.YearStart;
            data.YearEnd = model.YearEnd;

            //Informal Education Experience
            data.HeldBy = model.HeldBy;
            data.StartDate = model.StartDate;
            data.EndDate = model.EndDate;
            data.Fee = model.Fee;
            data.Description = model.Description;
            data.Certificate = model.Certificate;

            //Working Experience
            data.Company = model.Company;
            data.JobPosition = model.JobPosition;
            data.FromJob = model.FromJob;
            data.ToJob = model.ToJob;

            DbSet.Update(data);
            return await DbContext.SaveChangesAsync();
        }
    }
}