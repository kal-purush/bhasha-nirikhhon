using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Entity;

using Hatfield.EnviroData.DataProfile.WQ.Models;


namespace Hatfield.EnviroData.DataProfile.WQ
{
    public class WaterQualityDataProfile : IWaterQualityDataProfile
    {
        private DbContext _dbContext;
        public WaterQualityDataProfile(DbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public IQueryable<Site> GetAllSites()
        {
            throw new NotImplementedException();
        }

        public Site SaveOrUpdateSite(Site site)
        {
            throw new NotImplementedException();
        }

        public IQueryable<Analyte> GetAllAnalytes()
        {
            throw new NotImplementedException();
        }

        public IQueryable<SamplingActivity> GetAllSamplingActivities()
        {
            throw new NotImplementedException();
        }

        public IQueryable<FieldWorkProduction> GetAllFieldWorkProductions()
        {
            throw new NotImplementedException();
        }

        public IQueryable<SamplingActivity> QuerySamplingActivities(DateTime startDateTime, DateTime endDateTime)
        {
            throw new NotImplementedException();
        }

        public IQueryable<SamplingActivity> QuerySamplingActivities(DateTime startDateTime, DateTime endDateTime, IEnumerable<Site> sites)
        {
            throw new NotImplementedException();
        }

        public bool SaveOrUpdateSamplingActivities(IEnumerable<SamplingActivity> samples)
        {
            throw new NotImplementedException();
        }

        public IQueryable<WaterQualitySample> QueryWaterQualityData(DateTime startDateTime, DateTime endDateTime)
        {
            throw new NotImplementedException();
        }

        public IQueryable<WaterQualitySample> QueryWaterQualityData(DateTime startDateTime, DateTime endDateTime, Site site)
        {
            throw new NotImplementedException();
        }

        public IQueryable<WaterQualitySample> QueryWaterQualityData(DateTime startDateTime, DateTime endDateTime, Analyte analyte)
        {
            throw new NotImplementedException();
        }

        public IQueryable<WaterQualitySample> QueryWaterQualityData(DateTime startDateTime, DateTime endDateTime, Site site, Analyte analyte)
        {
            throw new NotImplementedException();
        }

        public IQueryable<WaterQualitySample> QueryWaterQualityData(DateTime startDateTime, DateTime endDateTime, IEnumerable<Site> sites, IEnumerable<Analyte> analytes)
        {
            throw new NotImplementedException();
        }

        public bool SaveOrUpdateWaterQualitySamples(IEnumerable<WaterQualitySample> samples)
        {
            throw new NotImplementedException();
        }
    }
}