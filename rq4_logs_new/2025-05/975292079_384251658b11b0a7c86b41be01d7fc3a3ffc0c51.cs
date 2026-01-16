using Libre_ERP_API.DTO_s;
using Libre_ERP_API.Services;

namespace Libre_ERP_API.Controllers
{
    public static class ProductEndpoints
    {
        public static void MapProductEndpoints(this IEndpointRouteBuilder app)
        {
            app.MapPost("/api/product", async (CreateProductRequest dto, ProductServices service) =>
            {
                try
                {
                    var result = await service.CreateProductAsync(dto);
                    return Results.Ok(new CreateProductResponse
                    {
                        IDProduct = result.IDReturn,
                        ErrorID = result.ErrorID,
                        ErrorDescription = result.ErrorDescription
                    });
                }
                catch (ArgumentException ex)
                {
                    return Results.BadRequest(new { error = ex.Message });
                }
            });
        }
    }
}