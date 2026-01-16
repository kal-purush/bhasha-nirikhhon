using DataAccess;
using HOApp.Model;
using System.Collections.ObjectModel;
using DataAccess.Help;
using DataAccess.Entity.Entities;
using System.Collections.Generic;
using System.Linq;
using System.Data.Entity;

namespace HOApp.ViewModel
{
    class StoreProductTransViewModel : NotifyUIBase
    {
        private RetailDbContext db = new RetailDbContext();

        public StoreProductTransViewModel(Product prod) : base()
        {
            LoadData(prod);
        }

        public ObservableCollection<StoreProductTranVM> StoreProductTrans { get; set; }
        public Product Product { get; set; }
        public void LoadData(Product prod)
        {
            Product = prod;
            ObservableCollection<StoreProductTranVM> _storeProductTrans = new ObservableCollection<StoreProductTranVM>();

            List<StoreProductTran> storeProductTrans = (from s in db.StoreProductTrans
                                                        where s.ProductID.Equals(prod.ProductID)
                                                        select s).ToList();
            foreach (StoreProductTran storeProductTran in storeProductTrans)
            {
                _storeProductTrans.Add(new StoreProductTranVM { IsNew = false, TheEntity = storeProductTran });
            }

            StoreProductTrans = _storeProductTrans;
            RaisePropertyChanged("StoreProductTrans");
        }

    }
}