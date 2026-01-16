using FDM90.Models;
using FDM90.Repository;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace FDM90.Handlers
{
    public class MarketingModelHandler : IMarketingModelHandler
    {
        private IRepository<MarketingModel> _marketingModelRepo;

        public MarketingModelHandler() : this(new MarketingModelRepository())
        {

        }

        public MarketingModelHandler(IRepository<MarketingModel> marketingModelRepo)
        {
            _marketingModelRepo = marketingModelRepo;
        }

        public IEnumerable<MarketingModel> GetAllMarketingModels()
        {
            return _marketingModelRepo.ReadAll();
        }
    }
}