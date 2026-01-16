using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AuctionNet.Interfaces;

namespace AuctionNet.Controllers
{
    public class SupplierController : ISupplierController
    {
        private AuctionNetModel _auctionNetModel;
        public void AddSupplier()
        {
            _auctionNetModel = new AuctionNetModel();
            throw new NotImplementedException();
        }

        public List<Suppliers> GetAll()
        {
            return _auctionNetModel.Suppliers.ToList();
        }

        //todo lösa error nedan (i if satsen specifikt)
        public List<ValidationResult> Create(Suppliers suppliers)
        {
            ValidationContext context = new ValidationContext(suppliers, null, null);
            List<ValidationResult> result = new List<ValidationResult>();
            bool valid = Validator.TryValidateObject(suppliers, context, result, true);
            if (valid)
            {
                _auctionNetModel.Suppliers.Add(suppliers);
                _auctionNetModel.SaveChanges();
            }
            return result;
        }

        public void ValidateInput()
        {
            throw new NotImplementedException();
        }
    }
}