namespace Movie.API.Controllers.v1;

[ApiController]
[Authorize(Roles = "Admin")]
[Route("api/v{version:apiVersion}/[controller]")]
[ApiVersion("1.0")]
public class GenreController : Controller
{
    private readonly ISender _sender;
    private readonly IMapper _mapper;

    public GenreController(ISender sender,
        IMapper mapper)
    {
        _sender = sender;
        _mapper = mapper;
    }

    [HttpPost]
    [ProducesResponseType(StatusCodes.Status201Created)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    public async Task<ActionResult<APIResponse>> CreateGenre([FromForm] CreateGenreRequest genre)
    {
        var command = new CreateGenreCommand(genre.Name);

        var result = await _sender.Send(command);

        var response = _mapper.Map<CreateGenreResponse>(result);

        var apiResponse = new APIResponse
        {
            Result = response,
            StatusCode = System.Net.HttpStatusCode.Created
        };

        return CreatedAtRoute("GetGenre", new { id = response.Id }, apiResponse);
    }

    [HttpGet("{id:int}", Name = "GetGenre")]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    [ProducesResponseType(StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public async Task<ActionResult<APIResponse>> GetGenre(int id)
    {
        var query = new GetGenreQuery(id);

        var genre = await _sender.Send(query);

        var apiResponse = new APIResponse
        {
            Result = genre,
            StatusCode = System.Net.HttpStatusCode.OK
        };

        return Ok(apiResponse);
    }

    [HttpGet(Name = "GetGenres")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    public async Task<ActionResult<APIResponse>> GetGenres([FromQuery] int pageNumber = 1,
        [FromQuery] int pageSize = 0)
    {
        var query = new GetGenresQuery
        {
            PageNumber = pageNumber,
            PageSize = pageSize
        };

        var result = await _sender.Send(query);

        var response = _mapper.Map<GetGenresResponse>(result);

        var apiResponse = new APIResponse
        {
            Result = response,
            StatusCode = System.Net.HttpStatusCode.OK
        };

        var pagination = _mapper.Map<Pagination>(query);

        if (pagination.PageSize > 0)
        {
            Response.Headers.Append("X-Pagination", JsonSerializer.Serialize(pagination));
        }

        return Ok(apiResponse);
    }
}