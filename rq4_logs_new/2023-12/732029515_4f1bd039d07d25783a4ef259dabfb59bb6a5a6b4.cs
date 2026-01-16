namespace ECommerce.API.Endpoints;

public class CustomerEndpoint : IEndPoint
{
    public void Register(WebApplication app) => app.Register<Customer, CustomerPostDTO, CustomerPutDTO, CustomerGetDTO>();
}