using System.Threading.Tasks;
using BookLib.GraphQLSchema;
using GraphQL;
using GraphQL.Types;
using Microsoft.AspNetCore.Mvc;


namespace BookLib.Controllers
{
    [Route("graphql")]
    [ApiController]
    public class GraphQLController : ControllerBase
    {

        public IDocumentExecuter DocumentExecuter { get; }
        public ISchema LibrarySchema { get; }

        public GraphQLController(ISchema librarySchema, IDocumentExecuter documentExecuter)
        {
            DocumentExecuter = documentExecuter;
            LibrarySchema = librarySchema;
        }

        [HttpPost]
        public async Task<IActionResult> Post([FromBody]GraphQLRequest query)
        {
            var result = await DocumentExecuter.ExecuteAsync(options =>
            {
                options.Schema = LibrarySchema;
                options.Query = query.Query;
            });

            if (result.Errors?.Count > 0)
            {
                return BadRequest(result);
            }

            return Ok(result);
        }
    }
}