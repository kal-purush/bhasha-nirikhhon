using Microsoft.AspNetCore.Mvc;

namespace AgeCalculateApi.Controllers;

[ApiController]
[Route("[controller]")]
public class AgeCalculateController : ControllerBase
{

    private readonly ILogger<AgeCalculateController> _logger;

    public AgeCalculateController(ILogger<AgeCalculateController> logger)
    {
        _logger = logger;
    }

    [HttpGet(Name = "AgeCalculate")]
    public string Get()
    {
        return "Storage Birthday is not setup yet";
    }

    [HttpPost(Name = "AgeCalculate")]
    public string AgeCalculation(string birthday)
    {
        if (String.IsNullOrEmpty(birthday))
        {
            throw new Exception("Birthday is empty!  Fix it!");
        }
        DateTime birthdayDate;
        bool isDate = DateTime.TryParse(birthday, out birthdayDate);
        if (isDate)
        {
            TimeSpan birthdayDiff = DateTime.Now.Subtract(birthdayDate);


            int daysOld = birthdayDiff.Days;
            int yearsOld = daysOld / 365;
            return yearsOld.ToString();
        }
        else{
            _logger.LogError("Birthday was not formatted correctly");
            return "birthday was not formatted correctly";
        }
    }
}