using System;
using System.Data.Entity;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Hatfield.EnviroData.Core;
using Hatfield.EnviroData.DataProfile.WQ.Models;

namespace Hatfield.EnviroData.DataProfile.WQ.Test
{
    using NUnit.Framework;

    [TestFixture]
    public class WaterQualityProfileIntegrationTest
    {
        private DbContext _dbContext;
        private IWaterQualityDataProfile _wqDataProfile;

        [OneTimeSetUp]
        public void Init()
        {
            _dbContext = new ODM2Entities();
            _wqDataProfile = new WaterQualityDataProfile(_dbContext);    
        }

        [OneTimeTearDown]
        public void Cleanup()
        {
            _wqDataProfile.Dispose();
        }

        [Test]
        public void SiteQueryTest()
        {            
            var sites = _wqDataProfile.GetAllSites();
            Assert.NotNull(sites);
            Assert.AreEqual(3, sites.Count());
        }
    }
}