using Restauracja.Model;
using Restauracja.Model.Entities;
using Restauracja.Services;
using Restauracja.Utilities;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace Restauracja
{
    public class RestaurantInitializer : CreateDatabaseIfNotExists<RestaurantDataContext>
    {
        protected override void Seed(RestaurantDataContext context)
        {
            List<ProductPOCO> newPOCOProducts = new List<ProductPOCO>()
                {
                    new Pizza("Margheritta", 20m,"Sos, ser mozarella, oregano"),
                    new Pizza("Vegetariana", 22m,"Bezmięsna"),
                    new Pizza("Tosca", 25m,"Z oliwą z oliwek"),
                    new Pizza("Venecia", 22m, "Mięsna"),

                    new PizzaTopping("Podwójny ser"),
                    new PizzaTopping("Salami"),
                    new PizzaTopping("Szynka"),
                    new PizzaTopping("Pieczarki"),

                    new MainCourse("Schabowy z frytkami/ryżem/ziemniakami", 30m),
                    new MainCourse("Ryba z frytkami", 28m),
                    new MainCourse("Placek po węgiersku", 27m),

                    new MainCourseSideDish("Bar sałatkowy", 5m),
                    new MainCourseSideDish("Zestaw sosów", 6m),

                    new Soup("Zupa pomidorowa", 12m),
                    new Soup("Rosół", 10m),

                    new Beverage("Kawa"),
                    new Beverage("Herbata"),
                    new Beverage("Cola")

                };

            foreach (var prod in newPOCOProducts)
            {
                var productEntity = new Product(prod.Name, prod.Price, prod.ProductType,
                prod.Description, prod.Remarks);

                context.Products.Add(productEntity);
            }

            try
            {
                context.SaveChanges();
            }
            catch (Exception ex)
            {
                Logger.Log(LogTarget.File, ex);
                Logger.Log(LogTarget.EventLog, ex);
                MessageBox.Show(@"Open 'Logs.txt' file in '\Data' installation foler or check Windows Event Viewer 'RestauracjaLogs' under 'Applications and Services logs' for details.", "Database seeding failed - no data created.");
            }
            //base.Seed(context);
        }
    }
}