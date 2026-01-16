using Microsoft.AspNetCore.Mvc;
using Domain.Interfaces;
using Domain.Entities.BaseEntities;
using System.Data;
using Newtonsoft.Json;
using CBEAPI.ViewModels;
using CBEAPI.Services;
using CBEAPI.Controllers;

namespace CBEAPI.API_Controllers.CBEAPIs
{

    [Route("api/[controller]")]
    [ApiController]
    public class Api_DashBoardController : Api_BaseController
    {
        private readonly IUnitOfWork _unitOfWork;
        public CustomResponseBuilder _resp;
        public Api_DashBoardController(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
            _resp = new CustomResponseBuilder();
        }


     






        [HttpGet]
        [Route("GetDashboard")]
        public ActionResult<IEnumerable<dynamic>> GetDashboard()
        {


            List<string> list = new List<string>();
           
            DataSet dataSet = _unitOfWork.ReportRepository.GetDataSetReport("SP_GetDAshboardData");

            foreach (DataTable  dt in dataSet.Tables)
            {
                String tabledata = DataTableToJSONWithJSONNet(dt);
                list.Add(tabledata);

            }

           


            return Ok(new { items = list); ;
        }
        public string DataTableToJSONWithJSONNet(DataTable table)
        {
            string JSONString = string.Empty;
            JSONString = JsonConvert.SerializeObject(table);
            return JSONString;
        }


    }
}