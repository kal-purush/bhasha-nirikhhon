using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using AlloyTemplates.Models.Pages;
using EPiServer.Core;
using EPiServer.Find.Cms;
using EPiServer.Find.Cms.Conventions;
using EPiServer.Find.Cms.Module;
using EPiServer.Framework;
using EPiServer.Framework.Initialization;

namespace AlloyTemplates.Business.Initialization
{
    [ModuleDependency(typeof(IndexingModule))]
    public class FindInitialization : IInitializableModule
    {
        public void Initialize(InitializationEngine context)
        {
            // Exclude everything. Assets folder, standalone blocks and some other items are by default indexed.
            ContentIndexer.Instance.Conventions.ForInstancesOf<IContent>().ShouldIndex(x => false);

            // include files/blobs
            ContentIndexer.Instance.Conventions.ForInstancesOf<MediaData>().ShouldIndex(x => true);

            // include regular pages that inherits our basepage, exluding special pages like settings and container pages
            ContentIndexer.Instance.Conventions.ForInstancesOf<SitePageData>().ShouldIndex(x => true);


        }

        public void Uninitialize(InitializationEngine context)
        {
           
        }
    }
}