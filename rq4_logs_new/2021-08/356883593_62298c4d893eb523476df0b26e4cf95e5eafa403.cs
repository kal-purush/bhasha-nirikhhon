using System;
using System.Collections.Generic;

namespace Com.Bateeq.Service.Warehouse.Lib.ViewModels.NewIntegrationViewModel
{
    public class ItemCoreViewModelUsername
    {
        public int _id { get; set; }
        public List<ItemViewModelRead> dataDestination { get; set; }

        public ItemArticleColorViewModel color { get; set; }


        public ItemArticleProcesViewModel process { get; set; }


        public ItemArticleMaterialViewModel materials { get; set; }


        public ItemArticleMaterialCompositionViewModel materialCompositions { get; set; }


        public ItemArticleCollectionViewModel collections { get; set; }


        public ItemArticleSeasonViewModel seasons { get; set; }

        public ItemArticleCounterViewModel counters { get; set; }


        public ItemArticleSubCounterViewModel subCounters { get; set; }


        public ItemArticleCategoryViewModel categories { get; set; }

        public double? DomesticCOGS { get; set; }

        public double? DomesticWholesale { get; set; }

        public double? DomesticRetail { get; set; }

        public double? DomesticSale { get; set; }

        public double? InternationalCOGS { get; set; }

        public double? InternationalWholesale { get; set; }

        public double? InternationalRetail { get; set; }

        public double? InternationalSale { get; set; }
        public double? price { get; set; }
        public string ImageFile { get; set; }
        public string Username { get; set; }
        public string Token { get; set; }
    }
}