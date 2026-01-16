using Bcp.Data;
using Bcp.Domain.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using AutoMapper;
using Bcp.Domain.Dtos;
using System.Linq;
// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Bcp.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class StockRemovalController : ControllerBase
    {
        private readonly BcpContext _context;
        private readonly IMapper _mapper;
        public StockRemovalController(BcpContext context, IMapper mapper)
        { 
            _context = context;
            _mapper = mapper;
        }

        [HttpPost("SaveUpdateStockRemoval")]
        public IActionResult SaveUpdateStockRemoval(StockWmsDto stockRemoval)
        {

            if (stockRemoval != null)
            {
                StockWmsDto data = (from stWms in _context.stockwms
                            where (stWms.LabelNr == stockRemoval.LabelNr && stWms.Product == stockRemoval.Product && stWms.Plant == stockRemoval.Plant && stWms.SapLoc == stockRemoval.SapLoc)
                            select new StockWmsDto
                            {
                                Id=stWms.Id,
                                Product=stWms.Product,
                                SapLoc=stWms.SapLoc,
                                LabelNr=stWms.LabelNr,
                                Plant=stWms.Plant
                            }).FirstOrDefault();
                if (data != null)
                {
                    data.Qty = stockRemoval.Qty;
                    data.Delivery = stockRemoval.Delivery;
                    var stockWmsDto = _mapper.Map<StockWms>(data);
                    _context.stockwms.Update(stockWmsDto);
                    _context.SaveChanges();
                }
                else
                {
                    var stockWmsDto = _mapper.Map<StockWms>(stockRemoval);
                    _context.stockwms.Add(stockWmsDto);
                    _context.SaveChanges();

                    var stockMovementsDto = _mapper.Map<StockMovements>(stockRemoval);
                    _context.stockmovements.Add(stockMovementsDto);
                    _context.SaveChanges();
                }           
            }
            return Ok();
        }
    }
}