using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WordPrediction.Application.Words.Queries.GetPredictWords;

namespace WordPrediction.Api.WordPredict
{
    [Route("api/[controller]s")]
    [ApiController]
    public class WordPredictController : ControllerBase
    {
        private readonly IGetPredictWordsListQuery _query;

        public WordPredictController(IGetPredictWordsListQuery query)
        {
            _query = query;
        }

        [HttpGet]
        public  IReadOnlyCollection<PredictWordModel> Get([FromQuery] string term)
        {
            return  _query.Execute(term);
        }
    }
}