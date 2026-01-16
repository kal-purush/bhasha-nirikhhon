using amazon_backend.CQRS.Queries.Request.ProductRequests;
using amazon_backend.Data;
using amazon_backend.Data.Dao;
using amazon_backend.Data.Entity;
using amazon_backend.Models;
using amazon_backend.Profiles.ProductProfiles;
using AutoMapper;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace amazon_backend.CQRS.Handlers.QueryHandlers.ProductHandlers.QueryHandlers
{
   
    public class GetProductBySlugQueryHandler:IRequestHandler<GetProductBySlugQueryRequest, Result<ProductViewProfile>>
    {
        private readonly IMapper _mapper;
        private readonly DataContext _dataContext;

        public GetProductBySlugQueryHandler(IMapper mapper, DataContext dataContext)
        {
            _mapper = mapper;
            _dataContext = dataContext;
        }

        public async Task<Result<ProductViewProfile>> Handle(GetProductBySlugQueryRequest request, CancellationToken cancellationToken)
        {
            var product = await _dataContext.Products
                    .Include(p => p.ProductImages)
                    .Include(p => p.Category)
                    .Include(p => p.ProductProperties)
                    .Include(p => p.AboutProductItems)
                    .Include(p => p.Reviews)
                    .Where(p => p.Slug == request.productSlug)
                    .AsSplitQuery()
                    .FirstOrDefaultAsync();
            if (product == null)
            {
                return new("Product not found") { statusCode = 404 };
            }
            return new(_mapper.Map<ProductViewProfile>(product)) { statusCode = 200 };
        }
    }
}