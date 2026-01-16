using LaserAPI.Logic;
using LaserAPI.Models.Helper.Laser;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;

namespace LaserAPI.Controllers
{
    [Route("test")]
    [ApiController]
    public class TestController : ControllerBase
    {
        private readonly LaserConnectionLogic _connectionLogic;

        public TestController(LaserConnectionLogic connectionLogic)
        {
            _connectionLogic = connectionLogic;
        }

        [HttpGet]
        public string Send()
        {
            const int durationMs = 2000;
            const int delayTime = 20000;
            int iterations = 0;

            Stopwatch stopwatch = Stopwatch.StartNew();

            while (stopwatch.ElapsedMilliseconds < durationMs)
            {
                iterations++;

                _connectionLogic.SendMessage(new LaserMessage
                {
                    RedLaser = 0,
                    BlueLaser = 6,
                    GreenLaser = 0,
                    X = -400,
                    Y = -400
                });
                long previousDelayTicks = stopwatch.ElapsedTicks;
                while (stopwatch.ElapsedTicks - previousDelayTicks < delayTime) { };
                _connectionLogic.SendMessage(new LaserMessage
                {
                    RedLaser = 0,
                    BlueLaser = 6,
                    GreenLaser = 0,
                    X = 400,
                    Y = 400
                });
                previousDelayTicks = stopwatch.ElapsedTicks;
                while (stopwatch.ElapsedTicks - previousDelayTicks < delayTime) { };
            }

            stopwatch.Stop();
            return $"ms average";
        }
    }
}