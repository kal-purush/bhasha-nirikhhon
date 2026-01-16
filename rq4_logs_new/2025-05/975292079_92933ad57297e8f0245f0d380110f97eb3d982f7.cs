using Libre_ERP_API.Data;
using Libre_ERP_API.DTO_s;
using Libre_ERP_API.Validators;
using Microsoft.Data.SqlClient;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;

namespace Libre_ERP_API.Services
{
    public class ProductServices
    {
        public async Task<(int IDReturn, int? ErrorID, string? ErrorDescription)> CreateProductAsync(CreateProductRequest dto)
        {
            ProductValidator.ValidatePrice(dto.Price);
        
            var parameters = new[]
            {
                new SqlParameter("@ID_USER", dto.IDUser),
                new SqlParameter("@NAME", dto.Name),
                new SqlParameter("@PRICE", dto.Price),
                new SqlParameter("@MIN_STOCK", dto.MinStock),
            };

            return await CommandHelpers.ExecuteNonQueryWithReturnAsync("SP_CREATE_PRODUCT", parameters);
        }
    }
}