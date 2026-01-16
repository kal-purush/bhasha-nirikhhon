using AutoMapper;
using Bcp.Data;
using Bcp.Domain.Dtos;
using Bcp.Domain.Models;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Bcp.Api.Services.Stock
{
    public class StockService : IStockService
    {
        private readonly BcpContext _context;
        private readonly IMapper _mapper;

        public StockService(BcpContext bcpContext, IMapper mapper)
        {
            _context = bcpContext;
            _mapper = mapper;
        }
        public int GetLabel()
        {
            try
            {
                Lblnbgen lbl = new Lblnbgen();
                _context.lblnbgens.Add(lbl);
                _context.SaveChanges();
                var res = lbl.LblId;
                _context.lblnbgens.Remove(lbl);
                _context.SaveChanges();
                return res;
            }
            catch (Exception ex)
            {
                return -1;
            }
        }

        public IQueryable<RackTypes> GetRackTypes() 
        {
            return _context.racktypes.AsQueryable();
        }

        public IQueryable<StorageLocation> GetStorageLocations()
        {
            return _context.storagelocation.AsQueryable();
        }

        public async Task SaveStock(StockWmsDto dto) 
        {
            try
            {
                var stockWms = _mapper.Map<StockWms>(dto);
                _context.stockwms.Add(stockWms);
                var stockMovements = _mapper.Map<StockMovements>(dto);
                _context.stockmovements.Add(stockMovements);
                await _context.SaveChangesAsync();
            }
            catch (Exception ex) 
            {
                
            }
        }
        public IQueryable<StockWms> GetExportData() 
        {
            try 
            {
                var data = (from stwms in _context.stockwms
                            join cls in _context.classifications on stwms.Product equals cls.Material
                            into clsTemp
                            from stCls in clsTemp.DefaultIfEmpty()
                            join slm in _context.saplocmarket on stwms.SapLoc equals slm.SapLoc
                            into slmTemp
                            from stSlm in slmTemp.DefaultIfEmpty()
                            select new StockWms
                            {
                                DryDate = stwms.DryDate,
                                MatType = stwms.MatType,
                                Bin = stwms.Bin,
                                ContentStatus = stwms.ContentStatus,
                                LabelNr = stwms.LabelNr,
                                Product = stwms.Product,
                                PrNetWeight = stwms.PrNetWeight,
                                PrWeightUnit = stwms.PrWeightUnit,
                                Qty = stwms.Qty,
                                RackType = stwms.RackType,
                                RackId = stwms.RackId,
                                SapLoc = stwms.SapLoc,
                                Plant = stwms.Plant,
                                Delivery = stwms.Delivery,
                                ProdAdd = stwms.ProdAdd,
                                ProdDsc2 = stwms.ProdDsc2,
                                SerialNr = stwms.SerialNr,
                                BatchNr = stwms.BatchNr,
                                MatGrp = stCls.MaterialNumber,
                                Market = stSlm.Market
                            }).AsQueryable();
                return data;
            }
            catch (Exception ex) {
                return null;
            }
        }
    }
}