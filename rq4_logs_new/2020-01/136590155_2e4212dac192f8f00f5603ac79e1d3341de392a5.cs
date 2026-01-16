using Com.Danliris.Sales.Test.BussinesLogic.DataUtils.FinishingPrintingCostCalculation;
using Com.Danliris.Sales.Test.BussinesLogic.DataUtils.FinisihingPrintingSalesContract;
using Com.Danliris.Sales.Test.BussinesLogic.Utils;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Facades.FinishingPrinting;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Facades.FinishingPrintingCostCalculation;
using Com.Danliris.Service.Sales.Lib.BusinessLogic.Facades.ProductionOrder;
using Com.Danliris.Service.Sales.Lib.Models.ProductionOrder;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Com.Danliris.Sales.Test.BussinesLogic.DataUtils.ProductionOrder
{
    public class ShinProductionOrderDataUtil : BaseDataUtil<ShinProductionOrderFacade, ProductionOrderModel>
    {
        private readonly FinishingPrintingCostCalculationFacade finishingPrintingCostCalculationFacade;
        private readonly ShinFinishingPrintingSalesContractFacade scFacade;
        public ShinProductionOrderDataUtil(ShinProductionOrderFacade facade, ShinFinishingPrintingSalesContractFacade _scFacade, FinishingPrintingCostCalculationFacade costCalculationFacade) : base(facade)
        {
            finishingPrintingCostCalculationFacade = costCalculationFacade;
            scFacade = _scFacade;
        }

        public override async Task<ProductionOrderModel> GetNewData()
        {
            ShinFinisihingPrintingSalesContractDataUtil scDU = new ShinFinisihingPrintingSalesContractDataUtil(scFacade, finishingPrintingCostCalculationFacade);
            var scData = await scDU.GetTestData();

            return new ProductionOrderModel()
            {
                SalesContractId = scData.Id,
                SalesContractNo = scData.SalesContractNo,
                AccountId = 1,
                AccountUserName = "name",
                ArticleFabricEdge = "e",
                BuyerCode = "cpde",
                BuyerId = 1,
                BuyerName = "name",
                BuyerType = "type",
                DeliveryDate = DateTimeOffset.UtcNow,
                DesignCode = "code",
                DesignMotiveCode = "cpde",
                DesignMotiveID = 1,
                DesignMotiveName = "a",
                DesignNumber = "number",
                DistributedQuantity = 1,
                FinishTypeCode = "code",
                FinishTypeName = "name",
                FinishTypeId = 1,
                FinishTypeRemark = "remar",
                MaterialCode = "code",
                MaterialId = 1,
                MaterialConstructionCode = "code",
                MaterialConstructionId = 1,
                MaterialPrice = 1,
                MaterialOrigin = "aa",
                MaterialName = "amm",
                MaterialConstructionRemark = "remark",
                MaterialTags = "tasg",
                OrderQuantity = 1,
                MaterialWidth = "ww",
                OrderTypeCode = "code",
                MaterialConstructionName = "name",
                OrderTypeId = 1,
                OrderTypeName = "name",
                OrderTypeRemark = "remar",
                OrderNo = scData.ProductionOrderNo,
                ProcessTypeCode = "cpde",
                Details = new List<ProductionOrder_DetailModel>()
                {
                    new ProductionOrder_DetailModel()
                    {
                        Quantity = 1,
                        ColorRequest = "e",
                        ColorTemplate = "e",
                        ColorType = "e",
                        ColorTypeId = "e",
                        UomId = 1,
                        UomUnit = "ee"
                    }
                },
                LampStandards = new List<ProductionOrder_LampStandardModel>()
                {
                    new ProductionOrder_LampStandardModel()
                    {
                        Name = "aa",
                        LampStandardId = 1,
                        Description ="e"
                    }
                },
                Run = "a",
                RunWidths = new List<ProductionOrder_RunWidthModel>()
                {
                    new ProductionOrder_RunWidthModel()
                    {
                        Value = 1
                    }
                },
                ShippingQuantityTolerance = 1,
                StandardTestCode = "c",
                ProfileLastName = "name",
                ProfileGender ="l",
                UnitId = 1,
                UnitName = "nmae",
                Sample = "samp",
                UomId = 1,
                UnitCode = "cde",
                YarnMaterialId = 1,
                YarnMaterialRemark = "rema",
                UomUnit  = "unit",
                Remark = "remar",
                StandardTestRemark = "remar",
                YarnMaterialCode = "code",
                StandardTestId =1,
                ProcessTypeName = "name",
                ProfileFirstName = "name",
                ProcessTypeId = 1,
                StandardTestName = "name",
                FinishWidth = "ee",
                PackingInstruction = "ins",
                ProcessTypeRemark = "remar",
                ShrinkageStandard ="ee",
                YarnMaterialName = "name",
                HandlingStandard = "ee"

            };
        }
    }
}