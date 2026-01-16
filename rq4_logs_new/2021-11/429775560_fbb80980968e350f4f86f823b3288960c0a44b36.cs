namespace ProjectBank.Server.Controllers;

[Authorize]
[ApiController]
[Route("api/[controller]")]
[RequiredScope(RequiredScopesConfigurationKey = "AzureAd:Scopes")]
public class KeywordController : ControllerBase 
{
    private readonly ILogger<KeywordController> _logger;
    private readonly IKeywordRepository _repository;

    public KeywordController(ILogger<KeywordController> logger, IKeywordRepository repository)
    {
        _logger = logger;
        _repository = repository;
    }    
}