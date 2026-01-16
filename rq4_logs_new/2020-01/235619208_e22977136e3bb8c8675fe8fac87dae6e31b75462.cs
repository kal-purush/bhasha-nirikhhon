using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using VRTest.Models;
using VRTest.Services;

namespace VRTest.Controllers
{
    public class NPVController : Controller
    {
        private readonly string _npvApiUrl;
        private readonly IHttpService _httpService;

        public NPVController(IConfiguration configuration,IHttpService httpService)
        {
            _npvApiUrl = configuration["Endpoints:vrtest.api.npv"];//npvApiUrl;
            _httpService = httpService;
        }

        public IActionResult Index( )
        {
            var jsonResult = TempData["npvResults"] as string;
            var npvs = JsonConvert.DeserializeObject<IList<NPVDetailModel>>(jsonResult);
            return View(npvs);
        }

      
        // GET: NPV/Create
        public IActionResult Create()
        {
            return View();
        }

        // POST: NPV/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Compute([Bind("CashFlowsDescription,Increment,UpperBoundDiscountRate,LowerBoundDiscountRate,InitialCost")] NPVModel nPVModel)
        {
            var cashFlows = nPVModel.CashFlowsDescription.Split(',').Select(double.Parse).ToList<double>();
            var requestModel = new NPVRequestModel
            {
                CashFlow = cashFlows
                ,Increment = nPVModel.Increment
                ,InitialCost = nPVModel.InitialCost
                ,LowerBoundDiscountRate = nPVModel.LowerBoundDiscountRate
                ,UpperBoundDiscountRate = nPVModel.UpperBoundDiscountRate
            };
            var npvResults = await _httpService.PostAsyncReturnAsJson<NPVRequestModel>(_npvApiUrl, requestModel);
            TempData["npvResults"] = npvResults;
            return RedirectToAction("Index");
        }

       
    }
}