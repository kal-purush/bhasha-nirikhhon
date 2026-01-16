using AqieHistoricaldataPerfBackend.Atomfeed.Models;
using AqieHistoricaldataPerfBackend.Atomfeed.Services;
using AqieHistoricaldataPerfBackend.Example.Services;
using Microsoft.AspNetCore.Mvc;
using static AqieHistoricaldataPerfBackend.Atomfeed.Models.AtomHistoryModel;

namespace AqieHistoricaldataPerfBackend.Atomfeed.Endpoints
{
    public static class AtomHistoryEndpoints
    {       
        public static void UseServiceAtomHistoryEndpoints(this IEndpointRouteBuilder app)
        {
            app.MapGet("AtomHistoryHealthchecks", GetHealthcheckdata);
            //app.MapGet("AtomHistoryHourlydata/{name}", GetHistorydataById);
            app.MapGet("AtomHistoryHourlydata", GetHistorydataById);
            app.MapPost("AtomHistoryHourlydata", GetHistorydataById);

        }

        private static async Task<IResult> GetHealthcheckdata(IAtomHistoryService Persistence)
        {
            var matches = await Persistence.AtomHealthcheck();
            return Results.Ok(matches);
        }

        //private static async Task<IResult> GetHistorydataById(string name, IAtomHistoryService Persistence)
        //{
        //    if (name is not null && !string.IsNullOrWhiteSpace(name))
        //    {
        //        var atomhourlyresult = await Persistence.GetAtomHourlydata(name);
        //        //return Results.File(atomhourlyresult,)
        //        return atomhourlyresult is not null ? Results.Ok(atomhourlyresult) : Results.NotFound();
        //    }
        //    else
        //    {
        //        return Results.NotFound();
        //    }
        //}
        private static async Task<IResult> GetHistorydataById([FromBody] querystringdata data,IAtomHistoryService Persistence, ILogger<AtomHistoryService> logger)
        {
            try
            {
                if (data is not null)
                {
                    var atomhourlyresult = await Persistence.GetAtomHourlydata(data);
                    //return Results.File(atomhourlyresult,)
                    return atomhourlyresult is not null ? Results.Ok(atomhourlyresult) : Results.NotFound();
                }
                else
                {
                    return Results.NotFound();
                }
            }
            catch(Exception ex)
            {
                logger.LogError("Error GetHistorydataById endpoints Info message {Error}", ex.Message);
                logger.LogError("Error GetHistorydataById endpoints Info stacktrace {Error}", ex.StackTrace);
                return Results.NotFound();

            }
        }
    }
}