using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

using JoyOI.Monitor.Models;

namespace JoyOI.Monitor.Controllers
{
    [Route("graph/statemachines/[controller]")]
    public class StateMachines : GraphController
    {
        const string MGMTSVC = "mgmtsvc";

        [HttpGet]
        public async Task<IActionResult> Created(int start, int end, int interval)
        {
            return Json(await GetGraphData(
                MGMTSVC,
                "SELECT " +
                "FLOOR(UNIX_TIMESTAMP(StartTime) / @interval) * @interval as t, " +
                "Count(Id) as c " +
                "FROM joyoi_mgmtsvc.statemachineinstances " +
                "GROUP BY t " +
                "ORDER BY t DESC",
                new GraphScaling(start, end, interval),
              (rows) => new Graph
              {
                  Title = "新建的状态机",
                  Data = new List<GraphData>() {
                          new GraphData {
                              Data = rows.Select(d => Tuple.Create(
                                  Convert.ToInt64(d["t"].ToString()),
                                  Convert.ToInt32(d["c"].ToString())
                              ))
                              .Where(t => t.Item1 >= start && t.Item1 <= end)
                              .Select(t => new DataPoint
                              {
                                  Timestamp = t.Item1,
                                  Value = t.Item2
                              })
                              .ToList()}
                        }
              }
            ));
        }
    }
}