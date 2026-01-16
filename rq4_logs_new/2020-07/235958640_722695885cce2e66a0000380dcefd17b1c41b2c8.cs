using Co.Id.Moonlay.Simple.Auth.Service.Lib.BusinessLogic.Interfaces;
using Co.Id.Moonlay.Simple.Auth.Service.Lib.Models;
using Co.Id.Moonlay.Simple.Auth.Service.Lib.Services.IdentityService;
using Co.Id.Moonlay.Simple.Auth.Service.Lib.Utilities;
using Com.Moonlay.Models;
using Com.Moonlay.NetCore.Lib;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Co.Id.Moonlay.Simple.Auth.Service.Lib.BusinessLogic.Services
{
    public class InformalEducationService : IInformalEducationService
    {
        private const string UserAgent = "auth-service";
        protected DbSet<InformalEducation> DbSet;
        protected IIdentityService IdentityService;
        public AuthDbContext DbContext;

        public InformalEducationService(IServiceProvider serviceProvider, AuthDbContext dbContext)
        {
            DbContext = dbContext;
            this.DbSet = dbContext.Set<InformalEducation>();
            this.IdentityService = serviceProvider.GetService<IIdentityService>();
        }

        public bool CheckDuplicate(int id)
        {
            return DbSet.Any(r => r.IsDeleted.Equals(false) && r.Id != id);
        }

        public async Task<int> CreateAsync(InformalEducation model)
        {
            EntityExtension.FlagForCreate(model, IdentityService.Username, UserAgent);
            DbSet.Add(model);
            return await DbContext.SaveChangesAsync();
        }

        public async Task<int> DeleteAsync(int id)
        {
            InformalEducation model = await ReadByIdAsync(id);
            EntityExtension.FlagForDelete(model, IdentityService.Username, UserAgent, true);
            DbSet.Update(model);
            return await DbContext.SaveChangesAsync();
        }

        public ReadResponse<InformalEducation> Read(int page, int size, string order, List<string> select, string keyword, string filter)
        {
            IQueryable<InformalEducation> query = DbSet.Where(x => !x.IsDeleted);
            List<string> searchAttributes = new List<string>()
            {
                "HeldBy"
            };
            query = QueryHelper<InformalEducation>.Search(query, searchAttributes, keyword);

            Dictionary<string, object> filterDictionary = JsonConvert.DeserializeObject<Dictionary<string, object>>(filter);
            query = QueryHelper<InformalEducation>.Filter((IQueryable<InformalEducation>)query, filterDictionary);

            List<string> selectedFields = new List<string>()
                {
                    "_id", "HeldBy",
                };
            Dictionary<string, string> orderDictionary = JsonConvert.DeserializeObject<Dictionary<string, string>>(order);
            query = QueryHelper<InformalEducation>.Order((IQueryable<InformalEducation>)query, orderDictionary);

            query = query.Select(x => new InformalEducation()
            {
                Id = x.Id,
                HeldBy = x.HeldBy,
                StartDate = x.StartDate,
                EndDate = x.EndDate,
                Description = x.Description,
                Certificate = x.Certificate
            });

            Pageable<InformalEducation> pageable = new Pageable<InformalEducation>(query, page - 1, size);
            List<InformalEducation> data = pageable.Data.ToList();
            int totalData = pageable.TotalCount;

            return new ReadResponse<InformalEducation>(data, totalData, orderDictionary, selectedFields);
        }

        public async Task<InformalEducation> ReadByIdAsync(int id)
        {
            var result = await DbSet.FirstOrDefaultAsync(d => d.Id.Equals(id) && !d.IsDeleted);
            return result;
        }

        public async Task<int> UpdateAsync(int id, InformalEducation model)
        {
            var data = await ReadByIdAsync(id);

            data.HeldBy = model.HeldBy;
            data.StartDate = model.StartDate;
            data.EndDate = model.EndDate;
            data.Description = model.Description;
            data.Certificate = model.Certificate;

            DbSet.Update(data);
            return await DbContext.SaveChangesAsync();
        }
    }
}