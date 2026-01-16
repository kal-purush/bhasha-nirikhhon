using EventCatalog.Domain;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Threading.Tasks;

namespace EventCatalog.Data
{
    public static class CatalogSeed
    {
        public static void Seed(CatalogContext context)
        {
            context.Database.Migrate(); // we need to run ADD-Migrate Powershell command
            if (!context.EventCategories.Any())
            {
                context.EventCategories.AddRange(GetPreConfiguredEventCategories());
                context.SaveChanges();
            }
            if (!context.EventTypes.Any())
            {
                context.EventTypes.AddRange(GetPreConfiguredEventTypes());
                context.SaveChanges();
            }
            if (!context.Events.Any())
            {
                context.Events.AddRange(GetPreConfiguredEvents());
                context.SaveChanges();
            }            
        }

        private static IEnumerable<EventsCatalog> GetPreConfiguredEvents()
        {
            return new List<EventsCatalog>()
            {
                new EventsCatalog(){ EventCategoryId=1, EventTypeId=2, Description = "Its Here! The JBF Issaquah Fall Event is right around the corner!", Name = "Just Between Family", StartDate=Convert.ToDateTime("09/20/2019"), EndDate=Convert.ToDateTime("09/23/2019"), Price = 15M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/1"},
                new EventsCatalog(){ EventCategoryId=2, EventTypeId=1, Description = "Its Here! The Fall fashion Event is right around the corner!", Name = "Fall Fashion Show", StartDate=Convert.ToDateTime("09/25/2019"), EndDate=Convert.ToDateTime("09/30/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/2"},
                new EventsCatalog(){ EventCategoryId=2, EventTypeId=1, Description = "Its Here! The Fake American Marriage show is right around the corner!", Name = "Fake American Marriage", StartDate=Convert.ToDateTime("09/25/2019"), EndDate=Convert.ToDateTime("09/25/2019"), Price = 35M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/3" },
                new EventsCatalog(){ EventCategoryId=2, EventTypeId=1, Description = "Its Here! The Miss Africa Washington Beauty Context is right around the corner!", Name = "Miss Africa Washington State", StartDate=Convert.ToDateTime("09/26/2019"), EndDate=Convert.ToDateTime("09/27/2019"), Price = 55M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/4" },
                new EventsCatalog(){ EventCategoryId=2, EventTypeId=1, Description = "Its Here! The Fall Gems Show is right around the corner!", Name = "Gems Show", StartDate=Convert.ToDateTime("10/05/2019"), EndDate=Convert.ToDateTime("10/06/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/5" },
                new EventsCatalog(){ EventCategoryId=3, EventTypeId=5, Description = "Its Here! The Fall Down Sound Tour is right around the corner!", Name = "Down Sound Tour", StartDate=Convert.ToDateTime("10/05/2019"), EndDate=Convert.ToDateTime("10/06/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/6" },
                new EventsCatalog(){ EventCategoryId=3, EventTypeId=5, Description = "Its Here! The Wild movie screening is right around the corner!", Name = "The Wild", StartDate=Convert.ToDateTime("10/07/2019"), EndDate=Convert.ToDateTime("10/07/2019"), Price = 35M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/7" },
                new EventsCatalog(){ EventCategoryId=4, EventTypeId=6, Description = "Its Here! The Norished festival is right around the corner!", Name = "Norished Festival", StartDate=Convert.ToDateTime("10/05/2019"), EndDate=Convert.ToDateTime("10/06/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/8" },
                new EventsCatalog(){ EventCategoryId=4, EventTypeId=6, Description = "Its Here! The kirkland Oktoberfest is right around the corner!", Name = "Kirkland Oktoberfest", StartDate=Convert.ToDateTime("10/07/2019"), EndDate=Convert.ToDateTime("10/08/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/9" },
                new EventsCatalog(){ EventCategoryId=5, EventTypeId=4, Description = "Its Here! The Fall Gems Show is right around the corner!", Name = "JukeBox The Ghost", StartDate=Convert.ToDateTime("10/15/2019"), EndDate=Convert.ToDateTime("10/15/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/10" },
                new EventsCatalog(){ EventCategoryId=5, EventTypeId=4, Description = "Its Here! The Mike Music Show is right around the corner!", Name = "Mike Music Show", StartDate=Convert.ToDateTime("10/17/2019"), EndDate=Convert.ToDateTime("10/17/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/11" },
                new EventsCatalog(){ EventCategoryId=5, EventTypeId=4, Description = "Its Here! The Love and Compromise music Show is right around the corner!", Name = "Love and Compromise", StartDate=Convert.ToDateTime("10/18/2019"), EndDate=Convert.ToDateTime("10/18/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/20" },
                new EventsCatalog(){ EventCategoryId=6, EventTypeId=3, Description = "Its Here! The Howard Cohen American Masters is right around the corner!", Name = "Howard Cohen American Masters", StartDate=Convert.ToDateTime("10/19/2019"), EndDate=Convert.ToDateTime("10/19/2019"), Price = 45M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/12" },
                new EventsCatalog(){ EventCategoryId=6, EventTypeId=3, Description = "Its Here! The FitBit Local is right around the corner!", Name = "Fit Bit Local", StartDate=Convert.ToDateTime("10/20/2019"), EndDate=Convert.ToDateTime("10/20/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/13" },
                new EventsCatalog(){ EventCategoryId=6, EventTypeId=3, Description = "Its Here! The Seattle Runners is right around the corner!", Name = "Seattle Runners", StartDate=Convert.ToDateTime("10/21/2019"), EndDate=Convert.ToDateTime("10/21/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/14" },
                new EventsCatalog(){ EventCategoryId=6, EventTypeId=3, Description = "Its Here! The Seattle Mountain Climbing is right around the corner!", Name = "Mountain Climbing", StartDate=Convert.ToDateTime("10/22/2019"), EndDate=Convert.ToDateTime("10/22/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/15" },
                new EventsCatalog(){ EventCategoryId=6, EventTypeId=3, Description = "Its Here! The Seattle Mountain film is right around the corner!", Name = "Mountain Film", StartDate=Convert.ToDateTime("10/23/2019"), EndDate=Convert.ToDateTime("10/23/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/16" },
                new EventsCatalog(){ EventCategoryId=6, EventTypeId=3, Description = "Its Here! The Bellevue Botanical tour is right around the corner!", Name = "Bellevue Botanical Tour", StartDate=Convert.ToDateTime("10/24/2019"), EndDate=Convert.ToDateTime("10/24/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/17" },
                new EventsCatalog(){ EventCategoryId=6, EventTypeId=3, Description = "Its Here! The Himalayan tour is right around the corner!", Name = "Himalayan", StartDate=Convert.ToDateTime("10/25/2019"), EndDate=Convert.ToDateTime("10/25/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/18" },
                new EventsCatalog(){ EventCategoryId=6, EventTypeId=3, Description = "Its Here! The Livin2travel goes alaska is right around the corner!", Name = "Livin2travel goes alaska", StartDate=Convert.ToDateTime("10/26/2019"), EndDate=Convert.ToDateTime("10/30/2019"), Price = 25M, PictureUrl = "http://externalcatalogbaseurltobereplaced/api/pic/19" }
            };
        }

        private static IEnumerable<EventType> GetPreConfiguredEventTypes()
        {
            return new List<EventType>()
            {
                new EventType(){Type="Appearance"},
                new EventType(){Type="Game"},
                new EventType(){Type="Tournment"},
                new EventType(){Type="Party"},
                new EventType(){Type="Tour"},
                new EventType(){Type="Festival"}
            };
        }

        private static IEnumerable<EventCategory> GetPreConfiguredEventCategories()
        {
            return new List<EventCategory>()
            {
                new EventCategory(){Name="Family & Education"},
                new EventCategory(){Name="Fashion"},
                new EventCategory(){Name="Film & Media"},
                new EventCategory(){Name="Food & Drink"},
                new EventCategory(){Name="Music"},
                new EventCategory(){Name="Sports & Fitness"},
                new EventCategory(){Name="Travel & OutDoor"}
            };
        }
    }
}