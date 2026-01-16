using Microsoft.AspNetCore.Mvc;
using WebShop.Models;
using System.Linq;
using WebShop.Models.MonthlyInstallments;
using System.Collections.Generic;

namespace WebShop.Controllers
{
    public class MonthlyInstallmentController : Controller
    {
        private readonly ShopDBContext dBContext;
        public MonthlyInstallmentController(ShopDBContext dBContext)
        {
            this.dBContext = dBContext;
        }


        public IActionResult Index()
        {
            var getInstallmentData = (from sale in dBContext.Tblsales
                        join cus in dBContext.Tblcustomers on sale.CustomerId equals cus.CustomerId
                        join pro in dBContext.Tblproducts on sale.ProductId equals pro.ProductId
                        orderby sale.SaleDate
                        where sale.Status.ToLower() == "active"
                        select new 
                        { 

                            sale.SaleId,
                            sale.ProductId,
                            sale.CustomerId,
                            sale.SaleTotalamount,
                            sale.SaleRemainingamount,
                            sale.SaleMonthlyinstallements,
                            sale.SalePaidAmount,
                            cus.CustomerName,
                            cus.CustomerFathername,
                            cus.CustomerMobileno,
                            pro.ProductName
                        }).ToList(); 

            List<MonthlyInstallment> mi = new List<MonthlyInstallment>();

            for(int i=0; i< getInstallmentData.Count(); i++)
            {
                MonthlyInstallment miobj = new MonthlyInstallment();

                miobj.SaleId            = getInstallmentData[i].SaleId;
                miobj.ProductId         = getInstallmentData[i].ProductId;
                miobj.CustomerId        = getInstallmentData[i].CustomerId;
                miobj.TotalAmount       = getInstallmentData[i].SaleTotalamount;
                miobj.RemainingAmount   = getInstallmentData[i].SaleRemainingamount;
                miobj.Installment       = getInstallmentData[i].SaleMonthlyinstallements;
                miobj.PaidAmount        = getInstallmentData[i].SalePaidAmount;
                miobj.CustomerName      = getInstallmentData[i].CustomerName;
                miobj.FatherName        = getInstallmentData[i].CustomerFathername;
                miobj.MobilleNo         = getInstallmentData[i].CustomerMobileno;
                miobj.ProductName       = getInstallmentData[i].ProductName;

                mi.Add(miobj);
            }

            return View(mi);
        }
    }
}