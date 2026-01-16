using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using AspnetFormDatatable.Supports;
public class DatatableDemoController : ApiController
{
    [HttpGet]
    public DatatableResponse<DatatableDemoListViewModel> List(int total, [FromUri]DatatableResquest datatableRequest)
    {
        var list = new List<DatatableDemoListViewModel>();
        for(long i = 1; i <= total; i++)
        {
            list.Add(new DatatableDemoListViewModel
            {
                Id = i,
                Name = $"Sample name {i}",
                Total = i * 10,
                CreatedDate = DateTime.Now.AddMinutes(-i),
            });
        }
        var dataTableResult = new DatatableResponse<DatatableDemoListViewModel>();
        dataTableResult.Draw = datatableRequest.Draw;
        dataTableResult.Data = list.Skip(datatableRequest.Start).Take(datatableRequest.Length);
        dataTableResult.RecordsTotal = list.Count();
        dataTableResult.RecordsFiltered = list.Count();
        return dataTableResult;
    }
}


public class DatatableDemoListViewModel
{
    public long Id { get; set; }
    public string Name { get; set; }
    public long Total { get; set; }
    public DateTime CreatedDate { get; set; }
}