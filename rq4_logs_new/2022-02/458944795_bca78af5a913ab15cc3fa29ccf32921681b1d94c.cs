using Microsoft.AspNetCore.Mvc;

namespace Week3.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class MainApi : ControllerBase
    {


        [HttpPost(Name = "GetWeatherForecast")]
        public ActionResult<List<string>> IntListWork(List<int> lint)
        {
            List<string> slist = new List<string>();

            double sum = 0;
            double counter = 0;
            double mean = 0;
           

            lint.Sort();
            foreach (int i in lint)
            {
                //if (counter == 0)
                //{
                //    slist.Add("List to small");
                //    counter++;
                //}
                //else
                //{
                    counter++;
                    sum += i;
                    mean = sum / counter;

                    slist.Add(counter + ": Current mean: " + mean);
                //}
            }
            Console.WriteLine("Sum: " + sum);

            return slist;
        }





    }
}