using Repair_Service.Models;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Repair_Service.Controllers
{
    public class BrandsPageController : PageController
    {

        public BrandsPageController() : base()
        {

        }

        public async Task<ObservableCollection<Brand>> GetBrandsAsync()
        {
            ObservableCollection<Brand> brands = new ObservableCollection<Brand>();
            await Task.Run(() => brands = database.GetBrands());
            return brands;
        }

        public async Task<bool> AddNewBrandAsync(Brand brand)
        {
            return await Task.Run(() => database.AddNewBrand(brand));
        }

        public async Task<bool> DeleteBrandAsync(Brand brand)
        {
            return await Task.Run(() => database.DeleteBrand(brand));
        }

    }
}