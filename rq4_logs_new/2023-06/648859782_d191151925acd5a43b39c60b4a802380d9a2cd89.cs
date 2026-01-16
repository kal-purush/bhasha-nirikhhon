namespace IDPLibrary.Entities.Interfaces.Endpoints;
public interface IJwksEndpoint
{
    Task<IResult> GetJWKDocumentAsync();

}