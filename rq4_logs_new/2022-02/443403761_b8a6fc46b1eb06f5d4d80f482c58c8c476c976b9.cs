using MediatR;
using products_core.Mediator.Queries;
using products_core.Models;
using products_core.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace products_core.Mediator.Handlers
{
    public class GetProductListQueryHandler : IRequestHandler<GetProductListQuery, List<ProductModel>>
    {
        private readonly IProductRepository _productRepository;

        public GetProductListQueryHandler(IProductRepository productRepository)
        {
            this._productRepository = productRepository;
        }

        public async Task<List<ProductModel>> Handle(GetProductListQuery request, CancellationToken cancellationToken)
        {
            return this._productRepository.GetProducts();
        }
    }
}